"""The compiler derives the scheduled/listener execution profile (issue #153).

A scheduled process and a listener process differ in their ``<process>``
attribute bytes (``allowSimultaneous``, ``updateRunDates``, and whether
``stopProcessingIfZeroDocuments`` appears at all). #153 moves that decision from
the legacy source-config sniff into the COMPILER, records it on the
materialization plan, and forbids the materializer from re-deriving it — so the
profile cannot contradict the graph it describes.

**Why these tests are shaped the way they are.** Lowering still refuses a
listener entry outright, so every root that compiles today derives
``"scheduled"``. A test suite written only against compilable inputs would
therefore pass identically against a hardcoded ``return "scheduled"``. The
listener cases below classify a CFG against a symbol table whose entry operation
carries a listener family — the case a constant implementation cannot pass, and
the reason this is a derivation rather than a default.
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


@pytest.mark.parametrize("family", sorted(LISTENER_CONNECTOR_TYPES))
def test_every_listener_family_derives_listener(family):
    """THE non-vacuity witness.

    A hardcoded ``return "scheduled"`` passes every other test in this file and
    fails every case here. Parametrized over the authority's FULL case set —
    ``LISTENER_CONNECTOR_TYPES`` itself — rather than a hand-picked example, so
    a family added to that set is covered automatically instead of silently
    dropping out of coverage.
    """
    cfg = _cfg()
    assert derive_process_execution_profile(cfg, _symbols(family)) == LISTENER


@pytest.mark.parametrize(
    "spelling", ["WSS", "  wss  ", "Web_Services", "  WSSSERVER  ", "Listener"]
)
def test_listener_detection_is_case_and_whitespace_insensitive(spelling):
    """Matched on the canonical, case-folded family — as lowering matches it.

    An exact lowercase comparison against the raw symbol value would let ``WSS``
    or a padded value slip past and emit a listener process with scheduled
    attribute bytes.
    """
    cfg = _cfg()
    assert derive_process_execution_profile(cfg, _symbols(spelling)) == LISTENER


def test_the_rule_uses_the_compilers_own_authority_not_a_second_copy():
    """No hand-written listener list in this module.

    Two copies of the family set would be free to disagree, and the
    disagreement would be a process emitted with the wrong ``<process>``
    attributes. Asserted against the source text so a future re-listing fails
    here rather than at a customer's process.
    """
    source = (
        Path(_src)
        / "boomi_mcp"
        / "compiler"
        / "process_ir"
        / "execution_profile.py"
    ).read_text()
    assert "LISTENER_CONNECTOR_TYPES" in source

    # ``"listener"`` is excluded from the sweep because it is ALSO the name of
    # the profile value itself (``LISTENER = "listener"``), which the module
    # legitimately defines. Every OTHER member of the authority set is a pure
    # connector-family name with no reason to appear here — if one does, it has
    # been hand-copied.
    hand_listable = sorted(LISTENER_CONNECTOR_TYPES - {"listener"})
    assert hand_listable, "positive control: the sweep must have something to check"
    for family in hand_listable:
        assert f'"{family}"' not in source, (
            f"{family!r} is hand-listed in execution_profile.py — it must come "
            "from LISTENER_CONNECTOR_TYPES"
        )


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
