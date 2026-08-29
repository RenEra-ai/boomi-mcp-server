"""The inventory rebaseline is a mechanism, so its own failures are pinned.

Written after the procedure was performed by hand three times and went wrong
three different ways, then after the script that replaced it was reviewed and
found to have two more failure modes of its own. Both of those are pinned here:
a check that could not fail, and a pair of generated artifacts that could be
left disagreeing about which tree they describe.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/rebaseline_m12_12_inventory.py"


def _function(name: str) -> ast.FunctionDef:
    tree = ast.parse(SCRIPT.read_text())
    return next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name
    )


def test_the_check_mode_can_fail():
    """A check that always exits zero reports nothing.

    The first version spliced in memory, compared only non-table lines, printed
    row counts that matched, and exited successfully — so the normal case, where
    values change without row counts changing, was invisible.
    """
    main = _function("main")
    returns = [
        node.value.value
        for node in ast.walk(main)
        if isinstance(node, ast.Return)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, int)
    ]
    assert 1 in returns, "no failing exit status: --check cannot report staleness"
    assert 0 in returns, "no succeeding exit status"


def test_neither_generated_artifact_is_written_before_both_are_validated():
    """A rebaseline that half-succeeds is worse than one that fails.

    The inventory used to be written before the markdown splice was validated,
    so any later refusal exited nonzero leaving the two artifacts describing
    different trees.
    """
    main = _function("main")
    writes = [
        node.lineno
        for node in ast.walk(main)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "write_text"
    ]
    grades = [
        node.lineno
        for node in ast.walk(main)
        if isinstance(node, ast.Raise)
        or (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "SystemExit"
        )
    ]
    assert writes, "the script writes nothing"
    assert grades, "the script validates nothing"
    assert min(writes) > max(grades), (
        "a tracked artifact is written before the last validation runs, so a "
        f"failure leaves the pair inconsistent (write at {min(writes)}, "
        f"last grade at {max(grades)})"
    )


def test_the_emitter_output_is_filtered_to_table_lines():
    """The emitter logs to stdout; only pipe-prefixed lines may be collected.

    This is what actually prevents log text being spliced into a tracked
    document — a failure that destroyed a heading in this repository once.
    """
    collect = _function("_emitted_blocks")
    guards = [
        node
        for node in ast.walk(collect)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "startswith"
    ]
    assert guards, "nothing restricts what is collected into a generated block"
