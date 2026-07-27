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


def _process_call_doc(*, wait, trailing=()):
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
            ]
            + list(trailing)
            + [{"kind": "stop"}],
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
# The authored schema currently GATES this shape: a process_call may live only in
# a pure process-call sequence and is rejected inside a Branch/Decision body, so
# no property read can follow a non-waiting call in any authorable document.
# Both facts were measured, not assumed:
#     root  [process_call, set_dpp] -> PROCESS_IR_CAPABILITY_UNSUPPORTED
#     leg   [process_call, set_dpp] -> PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
# The CFG can still represent it, so the rule is proven here rather than dropped.
# See the reachability note in effects.py for why it is pre-positioned.
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

    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "process_call", "process_ref": "$ref:c1", "wait": first_wait,
                 "abort_on_error": False},
                {"kind": "process_call", "process_ref": "$ref:c2", "wait": True,
                 "abort_on_error": False},
                {"kind": "stop"},
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
    return {"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "branch",
         "legs": [{"steps": leg, "terminal": {"kind": "stop"}} for leg in legs]}]}}


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
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        _pc("$ref:u", False), _pc("$ref:w", True), _pc("$ref:r", True),
        {"kind": "stop"}]}}
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:w", "$ref:r"],
        # deliberately NO summary for $ref:u — its effects are unknown
        [_sub("$ref:w", writes=(("dpp", "A"),)), _sub("$ref:r", reads=(("dpp", "A"),))],
    )
    assert codes == set()


def test_an_async_calls_own_declared_write_never_exempts_its_racer():
    """The discriminator that keeps the fix above from disarming the check: a
    non-waiting call's declared writes are the hazard, not a remedy for it."""
    doc = {"version": "1", "body": {"kind": "sequence", "steps": [
        _pc("$ref:u", False), _pc("$ref:r", True), {"kind": "stop"}]}}
    codes = _orch_codes(
        doc, ["$ref:u", "$ref:r"],
        [_sub("$ref:u", writes=(("dpp", "A"),)), _sub("$ref:r", reads=(("dpp", "A"),))],
    )
    assert PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNSAFE in codes
