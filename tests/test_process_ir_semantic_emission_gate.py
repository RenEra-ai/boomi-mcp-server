"""The canonical emission gate (issue #143, M12.8) — slice 9.

DELIBERATE DEVIATION from the architect plan, recorded here because a reader
comparing plan to code deserves the reason rather than a surprise.

The plan placed this gate inside ``compile_process_ir_v1``. That was implemented
and measured, and it breaks the legacy parity suites for TWO independent reasons.

``emission.py``'s own docstring is AUTHORITATIVE for this rationale — it sits with
the code it explains. Summarised here only so a reader of this suite knows why it
tests the boundary it does:

1. Canonical validation is deliberately stricter than the legacy surface, which
   keeps its behaviour through exemptions keyed on ADAPTER IDENTITY.
   ``compile_process_ir_v1(ir, symbols)`` does not know which adapter produced
   its IR, so it cannot look a policy up.
2. Covered by NO exemption: the compiler's own fixtures use placeholder component
   types (``component_type="t"``), which the reference phase reports as
   ``…REFERENCE_COMPONENT_TYPE_MISMATCH``.

A faithful reproduction measures 20 failing tests — 17 / 7 / 3 across
``…LINEAGE_CACHE_WRITER_MISSING``, ``…LINEAGE_PROPERTY_READ_BEFORE_WRITE`` (7
tests carry both) and ``…REFERENCE_COMPONENT_TYPE_MISMATCH``.

An earlier version of this docstring said the failures were "exactly" the two
exemption-covered codes, and added that this "identifies the cause rather than
merely correlating with it". Both were wrong: the third code is exempted by
nothing, so a policy-aware compiler gate would still reject those three.

``emit_legacy_result`` is the single production entry into the canonical
``compile -> emit`` chain and it DOES know its dialect, so the gate lives there.
With the policies applied, all 40 raw-byte XML goldens and every parity suite
stay green.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir.legacy_adapters import emission as emission_mod
from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (
    registered_adapters,
)


def test_the_gate_is_skipped_when_no_dialect_is_named():
    """``dialect=None`` means no policy can be looked up. Running strictly there
    would apply canonical rules to a legacy payload with its exemptions
    unavailable — the exact mismatch that makes a compiler-internal gate
    unshippable. Skipping is the deliberate, documented choice."""
    import inspect

    source = inspect.getsource(emission_mod.emit_legacy_result)
    assert "if dialect is not None:" in source
    assert "_enforce_semantic_report" in source


def test_the_gate_runs_after_compilation_and_before_emission():
    """Placement is the whole point: a semantic defect must be decided before
    any XML is generated, so nothing downstream is built from a rejected
    payload."""
    import inspect

    source = inspect.getsource(emission_mod.emit_legacy_result)
    compile_at = source.index("compile_process_ir_v1(")
    gate_at = source.index("_enforce_semantic_report(")
    emit_at = source.index("emit_process(")
    assert compile_at < gate_at < emit_at


def test_the_gate_validates_the_cfg_that_was_just_lowered():
    """Not a fresh lowering. The gate must judge exactly the graph about to be
    emitted, not one that merely ought to be identical."""
    import inspect

    source = inspect.getsource(emission_mod.emit_legacy_result)
    assert "cfg, plan = compile_process_ir_v1(" in source
    assert "_enforce_semantic_report(result, symbols, cfg, dialect)" in source


def test_every_production_call_site_names_a_registered_dialect():
    """A dialect string that matches no registered policy would silently run the
    gate with NO exemptions — the compiler-internal failure mode, reintroduced
    one layer up. Every site must name a policy that actually exists."""
    builder = (
        _ROOT
        / "src"
        / "boomi_mcp"
        / "categories"
        / "components"
        / "builders"
        / "process_flow_builder.py"
    ).read_text()

    named = set()
    for token in builder.split("dialect=")[1:]:
        named.add(token.split(")")[0].split(",")[0].strip().strip('"').strip("'"))

    assert named, "no production call site names a dialect"
    for dialect in named:
        assert dialect in registered_adapters(), dialect


def test_all_three_migrated_dialects_are_wired():
    builder = (
        _ROOT
        / "src"
        / "boomi_mcp"
        / "categories"
        / "components"
        / "builders"
        / "process_flow_builder.py"
    ).read_text()
    for dialect in ("flow_sequence", "wrapper_subprocess", "sync_pipeline"):
        assert 'dialect="{0}"'.format(dialect) in builder, dialect


def test_the_gate_raises_the_existing_compiler_error_type():
    """The emission contract is unchanged: callers still catch
    ``ProcessIRCompileError`` and still get it wrapped as
    ``LEGACY_ADAPTER_OUTPUT_PARITY_FAILED``. A new exception type here would
    escape every existing handler."""
    import inspect

    source = inspect.getsource(emission_mod._enforce_semantic_report)
    assert "raise ProcessIRCompileError(" in source
    assert "CompilerDiagnostic(" in source


def test_findings_convert_losslessly_into_compiler_diagnostics():
    import inspect

    source = inspect.getsource(emission_mod._enforce_semantic_report)
    for field in ("code=", "path=", "node_identity=", "message=", "remediation="):
        assert field in source, field


def test_only_errors_block_at_the_emission_boundary():
    import inspect

    source = inspect.getsource(emission_mod._enforce_semantic_report)
    assert "if not report.errors:" in source
    assert "return" in source


def test_the_policy_is_applied_before_the_block_decision():
    """If the block decision came first, the exemptions would never fire and
    every legacy golden would break — which is exactly what was measured when
    the gate lived in the compiler."""
    import inspect

    source = inspect.getsource(emission_mod._enforce_semantic_report)
    apply_at = source.index("apply_policy(")
    decide_at = source.index("if not report.errors:")
    assert apply_at < decide_at


def test_the_compiler_pipeline_itself_stays_ungated():
    """Direct callers of ``compile_process_ir_v1`` keep their existing contract.
    This is the deviation, pinned so it cannot be undone by accident."""
    compiler_pipeline = (
        _ROOT
        / "src"
        / "boomi_mcp"
        / "compiler"
        / "process_ir"
        / "pipeline.py"
    ).read_text()
    assert "semantic_validation" not in compiler_pipeline
