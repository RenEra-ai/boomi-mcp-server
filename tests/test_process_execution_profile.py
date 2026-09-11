"""The compiler derives the scheduled/listener execution profile (#153, #158).

A scheduled process and a listener process differ in their ``<process>``
attribute bytes (``allowSimultaneous``, ``updateRunDates``, and whether
``stopProcessingIfZeroDocuments`` appears at all). #153 moved that decision from
the legacy source-config sniff into the COMPILER, records it on the
materialization plan, and forbids the materializer from re-deriving it — so the
profile cannot contradict the graph it describes.

**What #158 changed.** Until #158 lowering refused every listener entry, so the
listener value could only be exercised by re-classifying a compiled CFG against a
symbol table whose entry operation carried a listener family. #158 added the
``listener`` entry node and made the compiler's entry policy the one authority:
a process is a listener exactly when its CFG ENTERS on that node. The family of
an operation symbol no longer decides anything — so the listener case below
compiles a real listener root, and the old family cases are inverted into the
regression that matters now: an unrelated WSS operation, in any spelling, cannot
make a scheduled root a listener.
"""

import sys
from pathlib import Path

import pytest

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)
sys.path.insert(0, str(Path(__file__).resolve().parent))

from boomi_mcp.compiler.process_ir.contracts import (  # noqa: E402
    LISTENER_CONNECTOR_TYPES,
    ComponentSymbolV1,
    SymbolTableV1,
)
from boomi_mcp.compiler.process_ir.execution_profile import (  # noqa: E402
    LISTENER,
    SCHEDULED,
    derive_process_execution_profile,
)
from boomi_mcp.compiler.process_ir.pipeline import compile_process_ir_v1  # noqa: E402
from boomi_mcp.models.process_ir import parse_process_ir_v1  # noqa: E402
from boomi_mcp.recipes.materialization import build_symbol_table  # noqa: E402

from _m12_11_support import VALID_IR_DOC, components  # noqa: E402


def _supporting():
    return [c for c in components() if c.type != "process"]


def _symbols(entry_family="database"):
    """A symbol table whose ENTRY operation carries ``entry_family``."""
    return build_symbol_table(
        _supporting(),
        connector_metadata={
            "db_op": (entry_family, "GET"),
            "api_op": ("http", "SEND"),
        },
    )


def _cfg():
    """A compiled CFG. Compiled with a NON-listener family, because lowering
    refuses a listener entry — which is exactly why the listener cases below
    re-classify this CFG against a different symbol table rather than trying to
    compile one."""
    ir = parse_process_ir_v1(dict(VALID_IR_DOC))
    cfg, _plan = compile_process_ir_v1(ir, _symbols())
    return cfg


def test_a_connector_source_entry_on_a_normal_family_is_scheduled():
    cfg = _cfg()
    assert derive_process_execution_profile(cfg, _symbols()) == SCHEDULED


def test_the_entry_node_is_read_from_the_cfgs_own_entry_id():
    """Not ``nodes[0]``. The compiler already decided which node is the entry."""
    cfg = _cfg()
    entry = [n for n in cfg.nodes if n.node_id == cfg.entry_node_id][0]
    assert entry.semantic.semantic_kind == "connector"
    assert entry.semantic.role == "source"


def _listener_root_and_symbols():
    ir = parse_process_ir_v1({"version": "1", "body": {"kind": "sequence", "steps": [
        {"kind": "listener", "operation_ref": "$ref:wss_op"},
        {"kind": "target", "connection_ref": "$ref:rest_conn",
         "operation_ref": "$ref:rest_op"},
        {"kind": "stop"},
    ]}})
    symbols = SymbolTableV1(symbols=(
        ComponentSymbolV1(ref="$ref:wss_op", component_id="WSS-OP",
                          component_type="connector-action", connector_type="wss",
                          action_type="Listen"),
        ComponentSymbolV1(ref="$ref:rest_conn", component_id="REST-C",
                          component_type="connector-settings", connector_type="rest"),
        ComponentSymbolV1(ref="$ref:rest_op", component_id="REST-O",
                          component_type="connector-action", connector_type="rest",
                          action_type="POST"),
    ))
    return ir, symbols


def test_listener_entry_derives_listener():
    """THE non-vacuity witness, now over a graph the compiler really produces.

    A hardcoded ``return "scheduled"`` passes every other test in this file and
    fails this one. It compiles a listener root end to end — the CFG, the emission
    plan whose Start is the fused listener form — and asks the profile of THAT
    CFG, so the witness cannot pass on a hand-built graph production never emits.
    """
    ir, symbols = _listener_root_and_symbols()
    cfg, plan = compile_process_ir_v1(ir, symbols)
    assert plan.nodes[0].emitter_input.emitter_kind == "start_listen"
    assert derive_process_execution_profile(cfg, symbols) == LISTENER


@pytest.mark.parametrize(
    "spelling",
    sorted(LISTENER_CONNECTOR_TYPES)
    + ["WSS", "  wss  ", "Web_Services", "  WSSSERVER  ", "Listener"],
)
def test_unrelated_wss_symbol_keeps_scheduled(spelling):
    """#158 inverted the old family cases, deliberately.

    Before #158 a scheduled CFG re-classified against a table whose ENTRY operation
    carried a listener family came out ``listener``. That was the rule then, and it
    is the regression now: the entry policy classifies by the entry NODE, so no
    operation family — in any spelling, trimmed or not, even on the entry's own
    operation — can make a scheduled root a listener and stamp it with listener
    ``<process>`` bytes. Parametrized over the compiler's whole listener-family
    refusal set plus the case/whitespace variants the old detection folded.
    """
    cfg = _cfg()
    assert derive_process_execution_profile(cfg, _symbols(spelling)) == SCHEDULED


def test_the_rule_uses_the_compilers_own_authority_not_a_second_copy():
    """ONE authority, and no hand-written listener list anywhere near it.

    #158 moved the rule into the compiler's entry policy, and this module now
    DELEGATES to it — a second classification here could disagree with the Start
    form the emission plan carries. Asserted against the source text, both
    directions: the delegation is present, and neither module hand-lists a
    connector-family spelling (the entry policy never consults a family at all).
    """
    base = Path(_src) / "boomi_mcp" / "compiler" / "process_ir"
    profile_source = (base / "execution_profile.py").read_text()
    policy_source = (base / "entry_policy.py").read_text()
    assert "classify_entry" in profile_source
    assert "def classify_entry" in policy_source

    # ``"listener"`` is excluded because it is ALSO the profile value and the
    # entry's semantic kind, which both modules legitimately name. Every OTHER
    # member of the refusal set is a pure connector-family spelling with no reason
    # to appear in either module — if one does, a family is being consulted again.
    hand_listable = sorted(LISTENER_CONNECTOR_TYPES - {"listener"})
    assert hand_listable, "positive control: the sweep must have something to check"
    for family in hand_listable:
        for name, source in (("execution_profile", profile_source),
                             ("entry_policy", policy_source)):
            assert f'"{family}"' not in source, (
                f"{family!r} is hand-listed in {name}.py — the profile is decided by "
                "the entry node, never by an operation family"
            )
    assert "LISTENER_CONNECTOR_TYPES" not in profile_source
    assert "LISTENER_CONNECTOR_TYPES" not in policy_source


def test_an_unresolvable_entry_operation_falls_back_to_scheduled():
    """Safe, and identical to pre-#153 bytes rather than a silent guess.

    ``scheduled`` is the default the legacy assembler has always emitted, so an
    entry this function cannot classify produces exactly the bytes it produced
    before #153. A genuinely unresolvable reference is already a compile error
    raised by lowering long before any profile matters.
    """
    cfg = _cfg()
    empty = build_symbol_table([], connector_metadata={})
    assert derive_process_execution_profile(cfg, empty) == SCHEDULED


def test_a_symbol_with_no_connector_family_is_scheduled():
    cfg = _cfg()
    symbols = build_symbol_table(_supporting(), connector_metadata={})
    assert derive_process_execution_profile(cfg, symbols) == SCHEDULED


def test_the_profile_values_are_exactly_two():
    """Closed set: a third value would mean a third ``<process>`` attribute set."""
    assert {SCHEDULED, LISTENER} == {"scheduled", "listener"}
