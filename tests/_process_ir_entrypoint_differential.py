"""Dual-entry-point differential driver for ProcessIR V1 — shared, not copied.

#178 established that `ProcessIRV1` is exported and MUTABLE, so a caller may parse
a legal document, mutate the model, and hand it straight to `compile_process_ir_v1`
— reaching the compile stages with a document the parser would have refused. Its
gate measures BOTH entry points on one model and compares the served diagnostic
identity.

#177 needs the same measurement with a different case set (the capability
manifest), and #177's own issue text says the two slices share this machinery **by
import rather than by merge**: the repo already keeps private test helpers as
underscore-prefixed modules (`_wave_gate_golden_corpus.py`,
`_m12_12_legacy_inventory.py`), so the driver moved here and both callers import it.
Copying it would have recreated the two-records-of-one-fact defect that #178 exists
to close — in the machinery built to detect it.

This module is deliberately NOT collected by pytest (underscore prefix). It holds no
module state: the compile-boundary swap is restored in a `finally`, so a caller may
interleave measurements freely.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir import pipeline as pl  # noqa: E402
from boomi_mcp.compiler.process_ir.diagnostics import (  # noqa: E402
    ProcessIRCompileError,
)
from boomi_mcp.models.process_ir import (  # noqa: E402
    ProcessIRValidationError,
    parse_process_ir_v1,
)

__all__ = [
    "GrammarBoundary",
    "diagnostic_vector",
    "measure_entrypoints",
]


class GrammarBoundary(ProcessIRCompileError):
    """Marks passage through the compile-side GRAMMAR boundary.

    It must SUBCLASS `ProcessIRCompileError`: `_guarded` re-raises that family
    untouched but converts anything else into a value-free
    `PROCESS_IR_COMPILE_INTERNAL` — measured, and it made every parser-accepted
    case look like a mismatch until the sentinel was given the right base.

    Raised in place of CFG lowering, so a case that reaches it passed the
    re-parse and `validate_body_capabilities` without being judged on symbol or
    semantic grounds the parser could not possibly reach.
    """

    def __init__(self):
        super().__init__([])


def diagnostic_vector(exc):
    """The full served identity, not just the code.

    `remediation` is compared too: it is served machine-facing text and was
    MEASURED to diverge between the two paths on corpus row 1, so a
    (code, path, message) assertion would leave a real divergence unpinned.
    """
    return tuple(
        (d.code, d.path, d.message, getattr(d, "remediation", None))
        for d in exc.diagnostics
    )


def measure_entrypoints(ir, *, mode="grammar", symbols=None, capabilities=None):
    """(parser_outcome, compiler_outcome) for one already-built model.

    `mode="grammar"` is #178's measurement, unchanged: CFG lowering is replaced by
    a `GrammarBoundary` raise, so a document that reaches lowering is reported as
    `("ACCEPTED",)` without being judged on symbol or semantic grounds the parser
    could not possibly reach. This is the mode that makes the two paths
    COMPARABLE — the parser has no symbol table, so letting compilation run on
    would compare a grammar verdict against a semantic one.

    `mode="full"` runs real compilation with the caller's `symbols`/`capabilities`.
    #177 needs it for capability witnesses whose refusal is only reachable after
    lowering (the semantic validators), where a grammar-boundary measurement would
    report `ACCEPTED` and prove nothing.

    Outcomes are one of `("ACCEPTED",)`, `("REFUSED",) + diagnostic_vector(exc)`,
    or — grammar mode only — `("REACHED-NO-BOUNDARY",)`, which means the boundary
    was never reached and is always a defect in the harness or the pipeline.
    """
    if mode not in ("grammar", "full"):
        raise ValueError("unknown mode {0!r}".format(mode))

    payload = ir.model_dump(mode="json", warnings=False)
    try:
        parse_process_ir_v1(copy.deepcopy(payload))
        parser = ("ACCEPTED",)
    except ProcessIRValidationError as exc:
        parser = ("REFUSED",) + diagnostic_vector(exc)

    if mode == "full":
        try:
            pl.compile_process_ir_v1(ir, symbols, capabilities=capabilities)
            compiler = ("ACCEPTED",)
        except ProcessIRCompileError as exc:
            compiler = ("REFUSED",) + diagnostic_vector(exc)
        return parser, compiler

    real = pl.lower_process_ir_to_cfg

    def _boundary(*_a, **_k):
        raise GrammarBoundary()

    pl.lower_process_ir_to_cfg = _boundary
    try:
        pl.compile_process_ir_v1(ir, None)
        compiler = ("REACHED-NO-BOUNDARY",)
    except GrammarBoundary:
        compiler = ("ACCEPTED",)
    except ProcessIRCompileError as exc:
        compiler = ("REFUSED",) + diagnostic_vector(exc)
    finally:
        pl.lower_process_ir_to_cfg = real
    return parser, compiler
