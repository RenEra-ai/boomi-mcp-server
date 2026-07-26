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
