"""Side-effect and replay safety (issue #143, M12.8) — slice 5. Still DARK.

The load-bearing assertion here is the NEGATIVE one: #142 already owns connector
retry safety, and this slice must not start second-guessing it. Every connector
code must keep firing from its own condition, and this module must add findings
only where #142 is silent — non-connector replay, and non-waiting subprocess
ordering.
"""

from __future__ import annotations

import json
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
from boomi_mcp.compiler.process_ir.semantic_validation.effects import (
    _replay_hazard,
    collect_effect_findings,
    collect_ordering_findings,
    collect_retry_effect_findings,
)
from boomi_mcp.errors import (
    PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN,
    PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE,
)
from boomi_mcp.models.process_ir import parse_process_ir_v1

_FIXTURES = _ROOT / "tests" / "fixtures" / "process_ir"
GOLDEN_DOCS = json.loads((_FIXTURES / "process_ir_v1.json").read_text())


def _prepared(doc):
    return prepare_validation_context(
        parse_process_ir_v1(doc), SymbolTableV1(symbols=())
    )


def _codes(doc):
    return {f.code for f in collect_effect_findings(_prepared(doc))}


def _process_call_doc(*, wait):
    # #175: a process-call root is the EXACT SINGLETON — the call ends its own
    # path, so the trailing stop this used to append is unauthorable. (The
    # `trailing=` parameter went with it: no call site ever passed one, and a
    # step after a call is now a continuation request. Downstream-read fixtures
    # use sibling branch legs instead — see `_async_pair`.)
    return {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "process_call",
                    "process_ref": "$ref:child",
                    "wait": wait,
                    "abort_on_error": False,
                }
            ],
        },
    }


def _read_dpp(name, target="OUT"):
    return {
        "kind": "set_dpp",
        "name": target,
        "source_values": [{"value_type": "dpp", "property_name": name}],
    }


# ---------------------------------------------------------------------------
# replay hazard classification
# ---------------------------------------------------------------------------


def test_a_cache_write_is_a_replay_hazard():
    class _S:
        semantic_kind = "cache_put"

    assert _replay_hazard(_S()) == "cache_write"


def test_a_persisted_property_write_is_a_replay_hazard():
    class _S:
        semantic_kind = "set_property"
        persist = True

    assert _replay_hazard(_S()) == "persisted_property"


def test_a_non_persisted_property_write_is_not_a_replay_hazard():
    """A non-persisted property dies with the execution, so replaying its write
    changes nothing. Flagging it would be noise on almost every payload."""

    class _S:
        semantic_kind = "set_property"
        persist = False

    assert _replay_hazard(_S()) is None


def test_a_plain_step_is_not_a_replay_hazard():
    class _S:
        semantic_kind = "message"

    assert _replay_hazard(_S()) is None


# ---------------------------------------------------------------------------
# retry regions
# ---------------------------------------------------------------------------


def test_a_flow_with_no_try_catch_produces_no_retry_findings():
    for name in GOLDEN_DOCS:
        prepared = _prepared(GOLDEN_DOCS[name])
        assert collect_retry_effect_findings(prepared) == (), name


def test_no_golden_doc_produces_a_blocking_effect_finding():
    """Warnings are expected — ``wrapper_flow`` legitimately contains a
    ``wait: false`` process call. What must never appear on a shipped golden is
    an ERROR, because that is what would block a build."""
    for name in GOLDEN_DOCS:
        blocking = [
            f
            for f in collect_effect_findings(_prepared(GOLDEN_DOCS[name]))
            if f.severity == "error"
        ]
        assert blocking == [], name


def test_the_wrapper_golden_does_warn_about_its_non_waiting_call():
    """Pins that the warning fires on a REAL document, not just a synthetic
    one — a rule proven only against hand-built input has not been shown to
    engage with anything the compiler actually produces."""
    codes = {f.code for f in collect_effect_findings(_prepared(GOLDEN_DOCS["wrapper_flow"]))}
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN in codes


# ---------------------------------------------------------------------------
# non-waiting subprocess ordering
# ---------------------------------------------------------------------------


def test_a_waiting_process_call_raises_no_ordering_finding():
    assert collect_ordering_findings(_prepared(_process_call_doc(wait=True))) == ()


def test_a_non_waiting_call_is_recorded_as_unknown_ordering():
    findings = collect_ordering_findings(_prepared(_process_call_doc(wait=False)))
    codes = {f.code for f in findings}
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN in codes


def test_the_unknown_ordering_finding_is_a_warning_not_a_block():
    """Unproven is not the same as wrong. Blocking here would reject every
    non-waiting call in the repo."""
    findings = collect_ordering_findings(_prepared(_process_call_doc(wait=False)))
    unknown = [
        f for f in findings if f.code == PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN
    ]
    assert unknown and unknown[0].severity == "warning"


# ---------------------------------------------------------------------------
# the UNSAFE branch, proven against a synthetic CFG
#
# These synthetic-CFG tests pin the shape independently of what the schema
# happens to allow. They are NOT the only evidence: the branch is reachable from
# the authored surface through typed contract reads — see
# `test_the_unsafe_branch_is_authorable` below, and the corrected reachability
# note in effects.py.
#
# What is gated is MIXING a process call with property steps, not the process
# call itself:
#     root  [process_call, set_dpp]      -> PROCESS_IR_CAPABILITY_UNSUPPORTED
#     root  [process_call, process_call] -> ALLOWED
#     leg   [process_call, set_dpp]      -> PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
#     leg   [process_call]               -> ALLOWED
# An earlier version of this block read the first two rows as "no read can follow
# a non-waiting call in any authorable document". That does not follow: the reads
# this collector scans come from a subprocess SUMMARY, not from a set_property
# node, so the gate on property steps never applied to them.
# ---------------------------------------------------------------------------


def _synthetic_call_then_read(*, property_name="FROM_CHILD", default=None, local_write=False):
    from boomi_mcp.compiler.process_ir.contracts import (
        CfgEdgeV1,
        CfgNodeV1,
        SemanticCfgV1,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.context import (
        PreparedProcessValidationV1,
        _edge_index,
    )

    source = {"value_type": "dpp", "property_name": property_name}
    if default is not None:
        source["default_value"] = default

    nodes = [
        CfgNodeV1(
            node_id="n1",
            ordinal=1,
            source_path="/body/steps/0",
            semantic={
                "semantic_kind": "process_call",
                "process_ref": "$ref:child",
                "wait": False,
                "abort_on_error": False,
            },
        ),
        CfgNodeV1(
            node_id="n2",
            ordinal=2,
            source_path="/body/steps/1",
            semantic={
                "semantic_kind": "set_property",
                "scope": "dpp",
                "name": "LOCAL" if local_write else "OUT",
                "persist": False,
                "source_values": [{"value_type": "static", "value": "v"}]
                if local_write
                else [source],
            },
        ),
        CfgNodeV1(
            node_id="n3",
            ordinal=3,
            source_path="/body/steps/2",
            semantic={
                "semantic_kind": "set_property",
                "scope": "dpp",
                "name": "OUT",
                "persist": False,
                "source_values": [{"value_type": "dpp", "property_name": "LOCAL"}],
            }
            if local_write
            else {"semantic_kind": "stop"},
            exit_role=None if local_write else "stop",
        ),
    ]
    edges = [
        CfgEdgeV1(
            edge_id="e1",
            ordinal=1,
            source_node_id="n1",
            target_node_id="n2",
            kind="ordering",
            local_ordinal=1,
            provenance_path="/body/steps/0",
        ),
        CfgEdgeV1(
            edge_id="e2",
            ordinal=2,
            source_node_id="n2",
            target_node_id="n3",
            kind="ordering",
            local_ordinal=1,
            provenance_path="/body/steps/1",
        ),
    ]
    cfg = SemanticCfgV1(
        entry_node_id="n1", nodes=tuple(nodes), edges=tuple(edges), exit_node_ids=()
    )
    return PreparedProcessValidationV1(
        ir=parse_process_ir_v1(GOLDEN_DOCS["wrapper_flow"]),
        cfg=cfg,
        symbols=SymbolTableV1(symbols=()),
        node_by_id={n.node_id: n for n in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={},
    )


def test_a_downstream_read_with_no_in_process_writer_is_demonstrably_unsafe():
    """The dependency is demonstrated: nothing in this process writes the key,
    so the only candidate writer is the call the flow does not wait for."""
    findings = collect_ordering_findings(_synthetic_call_then_read())
    unsafe = [
        f for f in findings if f.code == PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE
    ]
    assert unsafe
    assert unsafe[0].severity == "error"


def test_a_downstream_read_that_the_process_itself_writes_is_not_unsafe():
    """An in-process writer removes the dependency on the child entirely."""
    findings = collect_ordering_findings(_synthetic_call_then_read(local_write=True))
    codes = {f.code for f in findings}
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE not in codes


def test_a_defaulted_downstream_read_is_not_unsafe():
    findings = collect_ordering_findings(_synthetic_call_then_read(default="fallback"))
    codes = {f.code for f in findings}
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE not in codes


def test_ordering_findings_carry_no_property_name_or_process_ref():
    findings = collect_ordering_findings(
        _synthetic_call_then_read(property_name="SENTINEL_PROP")
    )
    assert findings
    for item in findings:
        blob = item.model_dump_json()
        assert "SENTINEL_PROP" not in blob
        assert "$ref:child" not in blob


# ---------------------------------------------------------------------------
# the negative assertion: #142 keeps its own ground
# ---------------------------------------------------------------------------


def test_this_module_emits_no_connector_retry_codes():
    """#142 owns connector retry safety. This slice must add findings only
    where #142 is silent — otherwise the same hazard reports twice under two
    codes and a caller cannot tell which to act on."""
    import boomi_mcp.compiler.process_ir.semantic_validation.effects as mod

    source = Path(mod.__file__).read_text()
    for owned_by_142 in (
        "PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION",
        "PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE",
        "PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING",
    ):
        assert owned_by_142 not in source, owned_by_142


# ---------------------------------------------------------------------------
# the retry rule FIRES — added after QA Bug #181
#
# The rule was implemented (effects.py) and its classifier was unit-tested, but
# nothing asserted the collector actually emits the code. "Implemented" and
# "proven to fire" are different claims, and the docs were making the second on
# the strength of the first. A synthetic CFG is required because no legacy
# dialect can project a Try/Catch region at all (QA Bug #176), so
# derive_error_regions returns empty on every legacy-projected graph.
# ---------------------------------------------------------------------------


def _try_catch_cfg(*, retry_count, hazard_kind="cache_put"):
    from boomi_mcp.compiler.process_ir.contracts import (
        CfgEdgeV1,
        CfgNodeV1,
        SemanticCfgV1,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.context import (
        PreparedProcessValidationV1,
        _edge_index,
    )

    if hazard_kind == "cache_put":
        hazard = {"semantic_kind": "cache_put", "cache_ref": "$ref:c"}
    else:
        hazard = {
            "semantic_kind": "set_property",
            "scope": "dpp",
            "name": "P",
            "persist": hazard_kind == "persisted",
            "source_values": [{"value_type": "static", "value": "v"}],
        }

    nodes = [
        CfgNodeV1(
            node_id="n1",
            ordinal=1,
            source_path="/body/steps/0",
            semantic={
                "semantic_kind": "try_catch",
                "scope": "process",
                "retry_count": retry_count,
            },
        ),
        CfgNodeV1(
            node_id="n2", ordinal=2, source_path="/body/steps/1", semantic=hazard
        ),
        CfgNodeV1(
            node_id="n3",
            ordinal=3,
            source_path="/body/steps/2",
            semantic={"semantic_kind": "stop"},
            exit_role="stop",
        ),
        CfgNodeV1(
            node_id="n4",
            ordinal=4,
            source_path="/body/catch_body/terminal",
            semantic={"semantic_kind": "stop"},
            exit_role="stop",
        ),
    ]
    edges = [
        CfgEdgeV1(
            edge_id="e1", ordinal=1, source_node_id="n1", target_node_id="n2",
            kind="ordering", local_ordinal=1, provenance_path="/body/steps/0",
        ),
        CfgEdgeV1(
            edge_id="e2", ordinal=2, source_node_id="n2", target_node_id="n3",
            kind="ordering", local_ordinal=1, provenance_path="/body/steps/1",
        ),
        CfgEdgeV1(
            edge_id="e3", ordinal=3, source_node_id="n1", target_node_id="n4",
            kind="catch", local_ordinal=2, provenance_path="/body/steps/0",
        ),
    ]
    cfg = SemanticCfgV1(
        entry_node_id="n1", nodes=tuple(nodes), edges=tuple(edges), exit_node_ids=()
    )
    return PreparedProcessValidationV1(
        ir=parse_process_ir_v1(GOLDEN_DOCS["linear_flow"]),
        cfg=cfg,
        symbols=SymbolTableV1(symbols=()),
        node_by_id={n.node_id: n for n in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={},
    )


def test_a_cache_write_inside_a_retried_region_fires_retry_effect_unsafe():
    findings = collect_retry_effect_findings(_try_catch_cfg(retry_count=3))
    assert [f.code for f in findings] == [PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE]
    assert findings[0].severity == "error"
    evidence = {e.key: e.value for e in findings[0].evidence}
    assert evidence == {"effect_kind": "cache_write", "retry_count": 3}


def test_a_persisted_property_write_inside_a_retried_region_also_fires():
    findings = collect_retry_effect_findings(
        _try_catch_cfg(retry_count=1, hazard_kind="persisted")
    )
    assert [f.code for f in findings] == [PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE]
    evidence = {e.key: e.value for e in findings[0].evidence}
    assert evidence["effect_kind"] == "persisted_property"


def test_a_zero_retry_region_does_not_fire():
    """Retry count zero is not a replay hazard — the region simply never repeats."""
    assert collect_retry_effect_findings(_try_catch_cfg(retry_count=0)) == ()


def test_a_non_persisted_write_inside_a_retried_region_does_not_fire():
    """It dies with the execution, so replaying it changes nothing."""
    findings = collect_retry_effect_findings(
        _try_catch_cfg(retry_count=3, hazard_kind="transient")
    )
    assert findings == ()


# ---------------------------------------------------------------------------
# F2 (repo Codex review): StateEffectV1.replay_safe was declared and never read
# ---------------------------------------------------------------------------


def test_a_declared_unsafe_effect_in_a_retried_region_fires():
    """A map contract saying replay_safe=False inside a positive-retry region
    was invisible: `_replay_hazard` recognised only explicit cache/persisted
    writes, so the flag existed and meant nothing."""
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        MapEffectContractV1,
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1,
    )

    prepared = _try_catch_cfg(retry_count=2, hazard_kind="transient")
    # replace the transient node with a map carrying an unsafe contract
    from boomi_mcp.compiler.process_ir.contracts import CfgNodeV1, SemanticCfgV1
    from boomi_mcp.compiler.process_ir.semantic_validation.context import (
        PreparedProcessValidationV1,
        _edge_index,
    )

    nodes = tuple(
        CfgNodeV1(
            node_id=n.node_id,
            ordinal=n.ordinal,
            source_path=n.source_path,
            semantic={"semantic_kind": "map", "map_ref": "$ref:m"}
            if n.node_id == "n2"
            else n.semantic,
            exit_role=n.exit_role,
        )
        for n in prepared.cfg.nodes
    )
    cfg = SemanticCfgV1(
        entry_node_id="n1", nodes=nodes, edges=prepared.cfg.edges, exit_node_ids=()
    )
    rebuilt = PreparedProcessValidationV1(
        ir=prepared.ir,
        cfg=cfg,
        symbols=SymbolTableV1(symbols=()),
        node_by_id={n.node_id: n for n in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={},
    )

    unsafe = ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(
                map_ref="$ref:m",
                effect=StateEffectV1(writes=(("dpp", "A"),), replay_safe=False),
            ),
        )
    )
    safe = ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(
                map_ref="$ref:m",
                effect=StateEffectV1(writes=(("dpp", "A"),), replay_safe=True),
            ),
        )
    )

    fired = {f.code for f in collect_retry_effect_findings(rebuilt, unsafe)}
    assert PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE in fired
    # and the flag genuinely discriminates
    assert collect_retry_effect_findings(rebuilt, safe) == ()
    # no contract at all -> opaque, still not a declared hazard
    assert collect_retry_effect_findings(rebuilt) == ()


# ---------------------------------------------------------------------------
# Codex review round 2 (#143). Four findings, all in the typed-contract path,
# plus one adjacent false positive found while verifying them.
# ---------------------------------------------------------------------------


def _map_retry_prepared(retry_count=3):
    """A retried region whose only member is a contracted MAP node."""
    from boomi_mcp.compiler.process_ir.contracts import (
        CfgEdgeV1,
        CfgNodeV1,
        SemanticCfgV1,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.context import (
        PreparedProcessValidationV1,
        _edge_index,
    )

    nodes = [
        CfgNodeV1(node_id="n1", ordinal=1, source_path="/body/steps/0",
                  semantic={"semantic_kind": "try_catch", "scope": "process",
                            "retry_count": retry_count}),
        CfgNodeV1(node_id="n2", ordinal=2, source_path="/body/steps/1",
                  semantic={"semantic_kind": "map", "map_ref": "$ref:m"}),
        CfgNodeV1(node_id="n3", ordinal=3, source_path="/body/steps/2",
                  semantic={"semantic_kind": "stop"}, exit_role="stop"),
        CfgNodeV1(node_id="n4", ordinal=4, source_path="/body/catch_body/terminal",
                  semantic={"semantic_kind": "stop"}, exit_role="stop"),
    ]
    edges = [
        CfgEdgeV1(edge_id="e1", ordinal=1, source_node_id="n1", target_node_id="n2",
                  kind="ordering", local_ordinal=1, provenance_path="/body/steps/0"),
        CfgEdgeV1(edge_id="e2", ordinal=2, source_node_id="n2", target_node_id="n3",
                  kind="ordering", local_ordinal=1, provenance_path="/body/steps/1"),
        CfgEdgeV1(edge_id="e3", ordinal=3, source_node_id="n1", target_node_id="n4",
                  kind="catch", local_ordinal=2, provenance_path="/body/steps/0"),
    ]
    cfg = SemanticCfgV1(entry_node_id="n1", nodes=tuple(nodes), edges=tuple(edges),
                        exit_node_ids=())
    return PreparedProcessValidationV1(
        ir=parse_process_ir_v1(GOLDEN_DOCS["linear_flow"]),
        cfg=cfg,
        symbols=SymbolTableV1(symbols=()),
        node_by_id={n.node_id: n for n in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={},
    )


def _map_caps(**effect_kwargs):
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        MapEffectContractV1,
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1,
    )

    return ProcessIRValidationCapabilitiesV1(
        map_effects=(
            MapEffectContractV1(map_ref="$ref:m", effect=StateEffectV1(**effect_kwargs)),
        )
    )


def test_an_unsafe_replay_contract_with_no_state_footprint_is_still_a_hazard():
    """`reads`/`writes` describe DDP/DPP/cache only. A script that posts to an
    external API has an EMPTY footprint, and `replay_safe` is the only field
    that can describe it — so gating the declaration on the footprint discarded
    it exactly where it carried all the information."""
    findings = collect_retry_effect_findings(
        _map_retry_prepared(), _map_caps(replay_safe=False)
    )
    assert [f.code for f in findings] == [PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE]


def test_a_replay_safe_contract_with_no_footprint_is_still_clean():
    """The discriminator: honouring the declaration must not mean flagging
    every contract in a retried region."""
    assert collect_retry_effect_findings(
        _map_retry_prepared(), _map_caps(replay_safe=True)
    ) == ()


def test_replay_safety_is_decided_by_the_flag_not_the_footprint():
    """Both footprints, both flags — the flag alone decides."""
    for kwargs in ({}, {"writes": (("dpp", "A"),)}):
        unsafe = collect_retry_effect_findings(
            _map_retry_prepared(), _map_caps(replay_safe=False, **kwargs)
        )
        safe = collect_retry_effect_findings(
            _map_retry_prepared(), _map_caps(replay_safe=True, **kwargs)
        )
        assert [f.code for f in unsafe] == [PROCESS_IR_SEMANTIC_RETRY_EFFECT_UNSAFE]
        assert safe == ()


# --- ordering: contract reads behind a non-waiting call --------------------


def _async_pair(reads, writes=(("dpp", "A"),), first_wait=False):
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1 as _ST,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1,
        SubprocessSummaryV1,
        validate_process_ir,
    )

    # #175: the orchestration shape moved from a ROOT CHAIN of calls to sibling
    # BRANCH LEGS, because a process call ends its path — a chain would need each
    # child's return paths bound to it. The hazard is unchanged and the fixture
    # is now the live-attested shape (capture §2.2 records Branch legs whose
    # bodies run process calls; it records no call wired onward to another).
    #
    # The race is just as real here: Boomi runs legs SEQUENTIALLY, in authored
    # order, and execution-scoped state written in an earlier leg is visible to a
    # later one. So leg 2 reading what leg 1's non-waiting child writes is the
    # same unordered read it always was — measured: the ordering finding still
    # fires for `first_wait=False`, and still does not for `first_wait=True`.
    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "branch",
                    "legs": [
                        {"steps": [], "terminal": {
                            "kind": "process_call", "process_ref": "$ref:c1",
                            "wait": first_wait, "abort_on_error": False}},
                        {"steps": [], "terminal": {
                            "kind": "process_call", "process_ref": "$ref:c2",
                            "wait": True, "abort_on_error": False}},
                    ],
                },
            ],
        },
    }
    ir = parse_process_ir_v1(doc)
    symbols = _ST(symbols=(
        ComponentSymbolV1(ref="$ref:c1", component_id="i1", component_type="process"),
        ComponentSymbolV1(ref="$ref:c2", component_id="i2", component_type="process"),
    ))
    caps = ProcessIRValidationCapabilitiesV1(subprocess_summaries=(
        SubprocessSummaryV1(process_ref="$ref:c1",
                            effect=StateEffectV1(writes=writes, replay_safe=True)),
        SubprocessSummaryV1(process_ref="$ref:c2",
                            effect=StateEffectV1(reads=reads, replay_safe=True)),
    ))
    return {f.code for f in validate_process_ir(ir, symbols, caps).errors}


def test_a_contract_read_behind_a_non_waiting_call_is_an_ordering_hazard():
    """The ordering collector scanned `_reads_of` only. A non-waiting call is
    authorable in an ORCHESTRATION root, where every sibling is itself a
    process_call and NO authored read can appear — so downstream of the call a
    contract read is the only kind of read there is, and lineage was proving it
    'established' by applying the child's writes synchronously."""
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in _async_pair(
        reads=(("dpp", "A"),)
    )


def test_a_contract_read_behind_a_WAITING_call_is_not_a_hazard():
    """The discriminator: with `wait=True` the child is ordered, so the
    identical contract pair carries no hazard at all. Without this, a collector
    that flagged every contract read would satisfy the test above."""
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE not in _async_pair(
        reads=(("dpp", "A"),), first_wait=True
    )


def test_a_key_the_child_provably_does_not_write_is_not_its_ordering_hazard():
    """Adjacent false positive, found while verifying the above and not part of
    the review. A declared summary is EXACT, so a read of a key outside it
    cannot race that call — yet it was reported as an ordering hazard while the
    lineage phase was already reporting the real defect (nothing writes it)."""
    codes = _async_pair(reads=(("dpp", "Z"),), writes=(("dpp", "A"),))
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE not in codes
    assert "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE" in codes


# ---------------------------------------------------------------------------
# Codex review round 3: both findings are consequences of scanning contract
# reads for ordering — one missed race, one manufactured one.
# ---------------------------------------------------------------------------


def _pc(ref, wait):
    return {"kind": "process_call", "process_ref": ref, "wait": wait,
            "abort_on_error": False}


def _sub(ref, *, reads=(), writes=()):
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        StateEffectV1,
        SubprocessSummaryV1,
    )

    return SubprocessSummaryV1(
        process_ref=ref,
        effect=StateEffectV1(reads=reads, writes=writes, replay_safe=True),
    )


def _orch_codes(doc, refs, summaries):
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ProcessIRValidationCapabilitiesV1,
        validate_process_ir,
    )

    symbols = SymbolTableV1(symbols=tuple(
        ComponentSymbolV1(ref=r, component_id="i{0}".format(n),
                          component_type="process")
        for n, r in enumerate(refs)
    ))
    report = validate_process_ir(
        parse_process_ir_v1(doc),
        symbols,
        ProcessIRValidationCapabilitiesV1(subprocess_summaries=tuple(summaries)),
    )
    return {f.code for f in report.errors}


def _branch_doc_of(legs):
    # #175: a process call is a leg TERMINAL, not a leg step. A leg whose last
    # element is a call therefore contributes `steps=leg[:-1], terminal=call`;
    # any other leg keeps its Stop terminal. The emitted graph is unchanged for
    # every non-call leg.
    def _leg(leg):
        if leg and leg[-1].get("kind") == "process_call":
            return {"steps": list(leg[:-1]), "terminal": leg[-1]}
        return {"steps": list(leg), "terminal": {"kind": "stop"}}

    return {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [_leg(leg) for leg in legs]}]}}


def _orch_doc(*calls):
    """An orchestration of N process calls that execute in the authored order.

    #175 replaced the ROOT CHAIN these tests used (`[call, call, ..., stop]`)
    with sibling Branch legs, because a call ends its own path — chaining would
    need each child's return paths bound to it, which is the gated capability.

    The execution semantics the tests depend on are unchanged: Boomi runs branch
    legs SEQUENTIALLY in authored order, and execution-scoped state written in an
    earlier leg is visible to a later one. So "call B runs after call A and can
    read what A wrote" holds exactly as it did in the chain — and this shape is
    the live-attested one (capture §2.2 records Branch legs running process
    calls, and no call wired onward to another).

    A Branch needs at least two legs, so a single call keeps the singleton root.
    """
    if len(calls) == 1:
        return {"version": "1", "body": {"kind": "sequence", "steps": [calls[0]]}}
    return _branch_doc_of([[call] for call in calls])


def test_a_non_waiting_call_races_a_contract_read_in_a_LATER_branch_leg():
    """Branch legs fan out in the CFG but run SEQUENTIALLY, so a leg-1 reader
    is not a graph descendant of a leg-0 call while still executing after it.
    A descendants-only scan reported nothing at all for this race."""
    codes = _orch_codes(
        _branch_doc_of([[_pc("$ref:a", False)], [_pc("$ref:b", True)]]),
        ["$ref:a", "$ref:b"],
        [_sub("$ref:a", writes=(("dpp", "A"),)), _sub("$ref:b", reads=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes


def test_a_reader_in_an_EARLIER_leg_is_an_order_defect_not_a_race():
    """Discriminator: reversing the legs is a different defect, and must not be
    relabelled as an ordering hazard of the async call."""
    codes = _orch_codes(
        _branch_doc_of([[_pc("$ref:b", True)], [_pc("$ref:a", False)]]),
        ["$ref:a", "$ref:b"],
        [_sub("$ref:a", writes=(("dpp", "A"),)), _sub("$ref:b", reads=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE not in codes


def test_a_later_leg_reading_an_unrelated_key_is_not_the_calls_hazard():
    """Discriminator: extending the scan must not flag every later-leg read."""
    codes = _orch_codes(
        _branch_doc_of([[_pc("$ref:a", False)], [_pc("$ref:b", True)]]),
        ["$ref:a", "$ref:b"],
        [_sub("$ref:a", writes=(("dpp", "A"),)),
         _sub("$ref:b", reads=(("dpp", "OTHER"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE not in codes


def test_a_WAITING_call_in_an_earlier_leg_creates_no_hazard():
    """Discriminator: the hazard is `wait=False`, not leg order."""
    codes = _orch_codes(
        _branch_doc_of([[_pc("$ref:a", True)], [_pc("$ref:b", True)]]),
        ["$ref:a", "$ref:b"],
        [_sub("$ref:a", writes=(("dpp", "A"),)), _sub("$ref:b", reads=(("dpp", "A"),))],
    )
    assert codes == set()


def test_a_trusted_synchronous_writer_answers_an_unknown_calls_race():
    """`in_process_writes` held only `_writes_of` results, so a key established
    by a WAITING child's exact summary looked unwritten and the unknown async
    call was blamed for it."""
    doc = _orch_doc(_pc("$ref:u", False), _pc("$ref:w", True), _pc("$ref:r", True))
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:w", "$ref:r"],
        # deliberately NO summary for $ref:u — its effects are unknown
        [_sub("$ref:w", writes=(("dpp", "A"),)), _sub("$ref:r", reads=(("dpp", "A"),))],
    )
    assert codes == set()


def test_an_async_calls_own_declared_write_never_exempts_its_racer():
    """The discriminator that keeps the fix above from disarming the check: a
    non-waiting call's declared writes are the hazard, not a remedy for it."""
    doc = _orch_doc(_pc("$ref:u", False), _pc("$ref:r", True))
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:r"],
        [_sub("$ref:u", writes=(("dpp", "A"),)), _sub("$ref:r", reads=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes


def test_a_contracts_own_write_does_not_exempt_its_own_read():
    """Codex review round 4. One `StateEffectV1` carries no intra-effect
    ordering, and lineage resolves that by checking reads against the incoming
    state before applying writes. The ordering collector's process-wide write
    union did not, so a contract declaring `reads=(A,), writes=(A,)` exempted
    its own read from the very race it was exposed to."""
    doc = _orch_doc(_pc("$ref:u", False), _pc("$ref:rw", True))
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:rw"],
        # no summary for the async call — its effects are unknown
        [_sub("$ref:rw", reads=(("dpp", "A"),), writes=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes


def test_a_separate_synchronous_writer_still_exempts_the_read():
    """The discriminator: excluding a node's OWN writes must not disturb the
    exemption a DIFFERENT trusted writer provides."""
    doc = _orch_doc(_pc("$ref:u", False), _pc("$ref:w", True), _pc("$ref:r", True))
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:w", "$ref:r"],
        [_sub("$ref:w", writes=(("dpp", "A"),)), _sub("$ref:r", reads=(("dpp", "A"),))],
    )
    assert codes == set()


def test_another_nodes_write_still_exempts_a_read_write_contract():
    """Codex review round 5. Excluding the reader's own writes by SUBTRACTING
    them from a flat key union discarded every other node's contribution to the
    same key, so a valid sequence — async call, a waiting writer of A, then a
    waiting contract that reads AND writes A — was rejected. Writers are
    tracked per node, because the question is "does some node OTHER THAN ME
    write this?" and a set of keys cannot answer it."""
    doc = _orch_doc(_pc("$ref:u", False), _pc("$ref:w", True), _pc("$ref:rw", True))
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:w", "$ref:rw"],
        [_sub("$ref:w", writes=(("dpp", "A"),)),
         _sub("$ref:rw", reads=(("dpp", "A"),), writes=(("dpp", "A"),))],
    )
    assert codes == set()


def test_the_self_write_exclusion_survives_the_per_node_rewrite():
    """The round-4 behaviour must not regress: with NO other writer, the
    read+write contract is still exposed to the race."""
    doc = _orch_doc(_pc("$ref:u", False), _pc("$ref:rw", True))
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:rw"],
        [_sub("$ref:rw", reads=(("dpp", "A"),), writes=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes


def test_the_unsafe_branch_is_authorable():
    """Pins the reachability measurements the module docstring states.

    The docstring previously claimed the UNSAFE branch could not be expressed by
    any authored document, and reasoned from two real rejections to a conclusion
    that did not follow. The reads this collector scans come from a subprocess
    SUMMARY, not from a `set_property` node, so a gate on mixing property steps
    with process calls never applied to them.

    A prose claim about what the schema accepts is exactly the kind that rots
    silently, so it is re-derived here instead of asserted in a comment.
    """
    from boomi_mcp.models.process_ir import ProcessIRValidationError

    pc = {"kind": "process_call", "process_ref": "$ref:a", "wait": False,
          "abort_on_error": False}
    pc2 = {"kind": "process_call", "process_ref": "$ref:b", "wait": True,
           "abort_on_error": False}
    read = {"kind": "set_dpp", "name": "OUT",
            "source_values": [{"value_type": "dpp", "property_name": "A"}]}

    def _parses(steps):
        try:
            parse_process_ir_v1({"version": "1",
                                 "body": {"kind": "sequence", "steps": steps}})
            return True
        except ProcessIRValidationError:
            return False

    def _leg_parses(legs):
        # #175: a call is a leg TERMINAL. `_branch_doc_of` applies that
        # convention, so this stays a statement about the legs, not the slots.
        return _parses(_branch_doc_of(legs)["body"]["steps"])

    # MIXING a call with property steps is gated, in either slot ...
    assert not _parses([pc, read, {"kind": "stop"}])
    assert not _leg_parses([[pc, read], [pc2]])
    # ... and #175 additionally gates CONTINUATION: a call ends its path, so a
    # root chain and a trailing stop are both refused. The pre-#175 spellings of
    # the two "allowed" rows below are kept here as the negative half, because
    # what this test exists to do is re-derive the reachability facts rather than
    # trust a comment about them.
    assert not _parses([pc, pc2, {"kind": "stop"}])
    assert not _parses([pc, {"kind": "stop"}])
    # ... while the call ITSELF remains authorable, as the exact-singleton root
    # and as a leg terminal. Both reach the collector, so the UNSAFE branch below
    # is still driven by a real document.
    assert _parses([pc])
    assert _leg_parses([[pc], [pc2]])


def test_the_unsafe_finding_comes_from_a_parsed_document_not_only_a_synthetic_cfg():
    """The consequence of the above: a real, parseable document reaches the
    ERROR branch. Without this the rule's only evidence is a hand-built CFG,
    which cannot show that the compiler ever produces the shape."""
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [], "terminal": _pc("$ref:a", False)},
            {"steps": [], "terminal": _pc("$ref:b", True)}]}]}}
    codes = _orch_codes(
        doc, ["$ref:a", "$ref:b"],
        [_sub("$ref:a", writes=(("dpp", "A"),)), _sub("$ref:b", reads=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes


def test_a_non_waiting_child_establishes_nothing_downstream_in_any_scope():
    """§6 architect review. `_trusted_effects` applied a subprocess summary's
    writes regardless of `wait`, so the lineage lattice treated a fire-and-forget
    child's declared write as established state.

    DPP and cache only LOOKED correct: the ordering collector happened to cover
    them. It deliberately skips DDP — document scope is not what an async race
    is about — so a `wait=False` child's declared DDP write fell through both
    checks and validated clean. The rule belongs in the lattice, where it holds
    for every scope at once.
    """
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ProcessIRValidationCapabilitiesV1,
        validate_process_ir,
    )

    def _report(scope, wait):
        doc = _orch_doc(_pc("$ref:a", wait), _pc("$ref:b", True))
        symbols = SymbolTableV1(symbols=(
            ComponentSymbolV1(ref="$ref:a", component_id="i1", component_type="process"),
            ComponentSymbolV1(ref="$ref:b", component_id="i2", component_type="process"),
        ))
        caps = ProcessIRValidationCapabilitiesV1(subprocess_summaries=(
            _sub("$ref:a", writes=((scope, "A"),)),
            _sub("$ref:b", reads=((scope, "A"),)),
        ))
        return validate_process_ir(parse_process_ir_v1(doc), symbols, caps)

    # EXECUTION-scoped state (process properties, the document cache) crosses
    # branch legs, because legs run sequentially inside one execution. So the
    # wait discriminator is exactly as before: a fire-and-forget child
    # establishes nothing, a waiting one does.
    for scope in ("dpp", "cache"):
        assert _report(scope, False).is_valid is False, scope
        # the discriminator: a WAITING child genuinely does establish it, and
        # a rule that rejected both would make every subprocess summary useless
        assert _report(scope, True).is_valid is True, scope

    # DDP is DOCUMENT-scoped, and #175's move to sibling legs makes that
    # visible: each leg receives an INDEPENDENT COPY of the document stream, so
    # a document property written in leg 0 is not the one leg 1 reads — whether
    # or not the child was waited on. The lattice says so with its own, more
    # specific code rather than the ordering finding, which is the stronger
    # statement: not "this race is unproven" but "this read can never be
    # established here at all".
    #
    # Measured, both directions, rather than assumed from the scope name.
    for wait in (False, True):
        report = _report("ddp", wait)
        assert report.is_valid is False, wait
        assert "PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID" in {
            f.code for f in report.errors
        }, wait


# ---------------------------------------------------------------------------
# QA round 16, mutation testing: three rules added in this issue were asserted
# by tests that could not detect their removal.
# ---------------------------------------------------------------------------


def test_an_async_writers_declared_write_never_exempts_another_calls_racer():
    """QA #201. The exclusion of a non-waiting call's own writes from
    `writers_by_key` had a "discriminator" that never reached it: that test gave
    the async call a SUMMARY, so the collector took the `summary is not None`
    arm and decided on `declared` alone. `writers_by_key` is consulted only when
    the call under investigation has NO summary.

    So the exclusion is exercised here by a second, LATER async call whose
    summary writes the key. Placing it after the reader keeps it from raising a
    hazard of its own, isolating the exemption path: delete the exclusion and
    that write lands in the index, exempts the read, and the finding vanishes.
    """
    doc = _orch_doc(
        _pc("$ref:u", False),   # unknown async call — no summary
        _pc("$ref:r", True),    # reads A
        _pc("$ref:v", False),   # async, DECLARES it writes A, and runs after
    )
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:r", "$ref:v"],
        [_sub("$ref:r", reads=(("dpp", "A"),)),
         _sub("$ref:v", writes=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes


def test_a_later_legs_membership_extends_past_its_first_node():
    """QA #202. `_leg_member_index` is "the single definition of what is in a
    leg", but every Branch test used SINGLE-STEP legs, so cutting the subtree
    walk left the whole suite green. The reader here is the SECOND step of the
    later leg, so it is found only if the walk continues."""
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        ProcessIRValidationCapabilitiesV1,
        validate_process_ir,
    )

    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [], "terminal": _pc("$ref:a", False)},
            {"steps": [
                {"kind": "set_dpp", "name": "FIRST",
                 "source_values": [{"value_type": "static", "value": "v"}]},
                {"kind": "set_dpp", "name": "OUT",
                 "source_values": [{"value_type": "dpp", "property_name": "A"}]},
            ], "terminal": {"kind": "stop"}}]}]}}
    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:a", component_id="i1", component_type="process"),))
    caps = ProcessIRValidationCapabilitiesV1(
        subprocess_summaries=(_sub("$ref:a", writes=(("dpp", "A"),)),))
    codes = {f.code for f in validate_process_ir(
        parse_process_ir_v1(doc), symbols, caps).errors}
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes


def test_execution_downstream_excludes_the_calls_own_leg():
    """QA #204. `other_ordinal > ordinal` selects strictly LATER legs. Relaxing
    it to `>=` pulls in the call's own leg — which holds nodes that run BEFORE
    it — and `discard(start)` removes only the call itself, so a reader earlier
    in the same leg becomes a hazard of a call it precedes.

    A synthetic CFG is required: the schema rejects a process_call that follows
    another step in a leg, the same reason the older tests here build one.
    """
    from boomi_mcp.compiler.process_ir.contracts import (
        CfgEdgeV1,
        CfgNodeV1,
        SemanticCfgV1,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.context import (
        PreparedProcessValidationV1,
        _edge_index,
    )
    from boomi_mcp.compiler.process_ir.semantic_validation.effects import (
        _execution_downstream,
    )

    nodes = [
        CfgNodeV1(node_id="n1", ordinal=1, source_path="/body/steps/0",
                  semantic={"semantic_kind": "branch", "leg_count": 2}),
        # leg 1: a reader, THEN the non-waiting call it precedes
        CfgNodeV1(node_id="n2", ordinal=2, source_path="/body/steps/0/legs/0/steps/0",
                  semantic={"semantic_kind": "set_property", "scope": "dpp", "name": "P",
                            "persist": False,
                            "source_values": [{"value_type": "static", "value": "v"}]}),
        CfgNodeV1(node_id="n3", ordinal=3, source_path="/body/steps/0/legs/0/steps/1",
                  semantic={"semantic_kind": "process_call", "process_ref": "$ref:c",
                            "wait": False, "abort_on_error": False}),
        # leg 2
        CfgNodeV1(node_id="n4", ordinal=4, source_path="/body/steps/0/legs/1/steps/0",
                  semantic={"semantic_kind": "message", "text": "x"}),
    ]
    edges = [
        CfgEdgeV1(edge_id="e1", ordinal=1, source_node_id="n1", target_node_id="n2",
                  kind="branch_leg", local_ordinal=1, leg_ordinal=1,
                  provenance_path="/body/steps/0"),
        CfgEdgeV1(edge_id="e2", ordinal=2, source_node_id="n2", target_node_id="n3",
                  kind="ordering", local_ordinal=1,
                  provenance_path="/body/steps/0/legs/0/steps/0"),
        CfgEdgeV1(edge_id="e3", ordinal=3, source_node_id="n1", target_node_id="n4",
                  kind="branch_leg", local_ordinal=2, leg_ordinal=2,
                  provenance_path="/body/steps/0"),
    ]
    cfg = SemanticCfgV1(entry_node_id="n1", nodes=tuple(nodes), edges=tuple(edges),
                        exit_node_ids=())
    prepared = PreparedProcessValidationV1(
        ir=parse_process_ir_v1(GOLDEN_DOCS["linear_flow"]),
        cfg=cfg, symbols=SymbolTableV1(symbols=()),
        node_by_id={n.node_id: n for n in cfg.nodes},
        outgoing=_edge_index(cfg.edges, "source_node_id"),
        incoming=_edge_index(cfg.edges, "target_node_id"),
        symbol_by_ref={})

    downstream = _execution_downstream(prepared, "n3")
    assert "n2" not in downstream, downstream   # ran BEFORE the call
    assert "n4" in downstream, downstream       # the strictly-later leg
    assert "n3" not in downstream


def test_the_unsafe_branch_is_producible_from_an_authorable_document():
    """§6 round 5. `…SIDE_EFFECT_ORDERING_UNSAFE` was listed as unreachable in
    the §10 census through three drafts, with two different wrong reasons. It is
    producible: a non-waiting call in leg 0 and an ordinary property read in a
    LATER leg, under the EMPTY default capabilities — legs run sequentially, so
    the child may still be running when leg 1 reads.

    Pinned as a test because a prose reachability claim is exactly what rots."""
    from boomi_mcp.compiler.process_ir.contracts import ComponentSymbolV1
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        DEFAULT_VALIDATION_CAPABILITIES,
        validate_process_ir,
    )

    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch", "legs": [
            {"steps": [], "terminal": _pc("$ref:child", False)},
            {"steps": [{"kind": "set_dpp", "name": "OUT", "source_values": [
                {"value_type": "dpp", "property_name": "A"}]}],
             "terminal": {"kind": "stop"}}]}]}}
    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:child", component_id="i1",
                          component_type="process"),))
    report = validate_process_ir(
        parse_process_ir_v1(doc), symbols, DEFAULT_VALIDATION_CAPABILITIES
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in {
        f.code for f in report.errors
    }
