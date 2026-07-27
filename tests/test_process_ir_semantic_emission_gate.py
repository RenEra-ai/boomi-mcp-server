"""The canonical semantic gate lives in the COMPILER (issue #143, M12.8).

This file previously pinned the opposite arrangement, and the story is worth
keeping because the reasoning was wrong in an instructive way.

The gate was first placed inside ``compile_process_ir_v1``, measured, and found
to break 27 tests. It was moved out to ``emit_legacy_result`` on the reasoning
that:

1. canonical validation is deliberately stricter than the legacy surface, which
   keeps its behaviour through exemptions keyed on ADAPTER IDENTITY, and
   ``compile_process_ir_v1(ir, symbols)`` cannot know which adapter produced its
   IR, so it cannot look a policy up; and
2. the compiler's own fixtures used placeholder component types, which the
   reference phase reported as ``…REFERENCE_COMPONENT_TYPE_MISMATCH``.

The §6 architect review rejected both, and re-measurement agreed:

* (1) is true and beside the point. The compiler cannot look the policy up — but
  the ADAPTER can, and can pass it. ``compile_process_ir_v1`` now takes a
  ``validation_policy`` keyword with a STRICT default; ``emit_legacy_result``
  passes ``lookup_policy(dialect)``. Of the 27 failures, **19 were fixed by that
  one change alone**.
* (2) was fixture debt, not an argument. ``tests/test_process_ir_compiler.py``
  typed every symbol ``"sentinel"``; typing them by ROLE fixed the rest.

Leaving the canonical path ungated meant a direct caller of the compiler got no
semantic validation at all — the acceptance criterion this issue exists to
satisfy. Gating it also immediately caught a real fixture bug that had been
invisible: the one ``cache_get`` in ``test_process_ir_rich_control_bodies.py``
named a PROCESS component as its document cache.

All 40 raw-byte XML goldens and every parity suite stay green.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "src")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from boomi_mcp.compiler.process_ir import pipeline as compiler_pipeline
from boomi_mcp.compiler.process_ir.legacy_adapters import emission as emission_mod
from boomi_mcp.compiler.process_ir.semantic_validation.validation_policy import (
    registered_adapters,
)


def test_the_gate_runs_inside_the_canonical_compiler():
    """The whole point of the relocation: every caller of the compiler is
    gated, not only the ones that happen to come through a legacy adapter."""
    import inspect

    source = inspect.getsource(compiler_pipeline.compile_process_ir_v1)
    # Dispatched THROUGH `_guarded`, so an unexpected collector failure becomes
    # a value-free PROCESS_IR_COMPILE_INTERNAL like every other stage.
    assert "_enforce_semantic_report," in source
    assert "_guarded(" in source


def test_the_gate_runs_before_any_emission_plan_exists():
    """"No plan lowering occurs with report errors" holds by construction: the
    gate sits between CFG lowering and plan lowering, so a rejected payload
    never reaches an emitter."""
    import inspect

    source = inspect.getsource(compiler_pipeline.compile_process_ir_v1)
    lowered_at = source.index("lower_process_ir_to_cfg")
    gate_at = source.index("_enforce_semantic_report,")
    plan_at = source.index("lower_cfg_to_emission_plan")
    assert lowered_at < gate_at < plan_at


def test_the_gate_validates_the_cfg_that_was_just_lowered():
    """Not a fresh lowering. The gate must judge exactly the graph about to be
    emitted, not one that merely ought to be identical."""
    import inspect

    source = inspect.getsource(compiler_pipeline._enforce_semantic_report)
    assert "validate_lowered_process_ir(" in source
    assert "ir, cfg, symbols, capabilities or DEFAULT_VALIDATION_CAPABILITIES" in source


def test_the_default_is_STRICT():
    """A caller who names no policy gets the canonical rules. The previous
    arrangement skipped the gate entirely in that case, which is how a direct
    compiler call ended up unvalidated."""
    import inspect

    signature = inspect.signature(compiler_pipeline.compile_process_ir_v1)
    assert signature.parameters["validation_policy"].default is None
    source = inspect.getsource(compiler_pipeline._enforce_semantic_report)
    assert "apply_policy(report, policy)" in source


def test_the_legacy_adapter_only_passes_its_identity_across():
    """The adapter no longer runs a gate of its own — it supplies the one fact
    the compiler cannot derive."""
    import inspect

    source = inspect.getsource(emission_mod.emit_legacy_result)
    assert "validation_policy=lookup_policy(dialect)" in source
    assert "_enforce_semantic_report" not in source


def test_every_production_call_site_names_a_registered_dialect():
    """A dialect string matching no registered policy would run the gate with
    NO exemptions. Every site must name a policy that actually exists."""
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
    """The compile contract is unchanged: callers still catch
    ``ProcessIRCompileError``. A new exception type here would escape every
    existing handler."""
    import inspect

    source = inspect.getsource(compiler_pipeline._enforce_semantic_report)
    assert "raise ProcessIRCompileError(" in source
    assert "CompilerDiagnostic(" in source


def test_findings_convert_losslessly_into_compiler_diagnostics():
    import inspect

    source = inspect.getsource(compiler_pipeline._enforce_semantic_report)
    for field in ("code=", "path=", "node_identity=", "message=", "remediation="):
        assert field in source, field


def test_only_errors_block_at_the_gate():
    import inspect

    source = inspect.getsource(compiler_pipeline._enforce_semantic_report)
    assert "if not report.errors:" in source
    assert "return" in source


def test_the_policy_is_applied_before_the_block_decision():
    """If the block decision came first, exemptions would never fire and every
    legacy golden would break — which is exactly what was measured when the
    gate was strict for everyone."""
    import inspect

    source = inspect.getsource(compiler_pipeline._enforce_semantic_report)
    apply_at = source.index("apply_policy(")
    decide_at = source.index("if not report.errors:")
    assert apply_at < decide_at


# ---------------------------------------------------------------------------
# behaviour, not just placement
# ---------------------------------------------------------------------------


def test_a_direct_compiler_call_is_now_rejected_for_a_semantic_defect():
    """The criterion, executed: a payload with a fatal semantic finding must not
    compile, even when no adapter and no dialect are involved."""
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1,
    )
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    doc = {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {
                    "kind": "source",
                    "connection_ref": "$ref:conn",
                    "operation_ref": "$ref:op",
                },
                # reads a DPP nothing ever writes
                {
                    "kind": "set_dpp",
                    "name": "OUT",
                    "source_values": [
                        {"value_type": "dpp", "property_name": "NEVER_WRITTEN"}
                    ],
                },
                {
                    "kind": "target",
                    "connection_ref": "$ref:tconn",
                    "operation_ref": "$ref:top",
                },
                {"kind": "stop"},
            ],
        },
    }
    symbols = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(ref=ref, component_id="id" + ref, component_type=ctype)
            for ref, ctype in (
                ("$ref:conn", "connector-settings"),
                ("$ref:op", "connector-action"),
                ("$ref:tconn", "connector-settings"),
                ("$ref:top", "connector-action"),
            )
        )
    )
    ir = parse_process_ir_v1(doc)
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compiler_pipeline.compile_process_ir_v1(ir, symbols)
    codes = [d.code for d in excinfo.value.diagnostics]
    assert "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE" in codes


def test_a_clean_document_still_compiles_through_the_gate():
    """The discriminator: a gate that rejected everything would satisfy the test
    above and break the entire compiler. Uses a shipped GOLDEN document, so this
    is the real compiler input, not a hand-rolled one."""
    import json

    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1,
    )
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    # `control_flow`, not `linear_flow`: the latter is a CODEC fixture with a
    # documented genuine read-before-write, pinned by
    # test_the_linear_golden_has_one_genuine_read_before_write.
    golden = json.loads(
        (_ROOT / "tests" / "fixtures" / "process_ir" / "process_ir_v1.json").read_text()
    )["control_flow"]
    ir = parse_process_ir_v1(golden)

    by_role = {
        "connection_ref": "connector-settings",
        "operation_ref": "connector-action",
        "map_ref": "transform.map",
        "cache_ref": "documentcache",
        "process_ref": "process",
    }
    refs = {}
    for node in lower_process_ir_to_cfg(ir).nodes:
        semantic = node.semantic
        for role, ctype in by_role.items():
            value = getattr(semantic, role, None)
            if isinstance(value, str) and value:
                refs[value] = ctype
        for container in ("steps", "source_values"):
            for item in getattr(semantic, container, ()) or ():
                nested = getattr(item, "profile_ref", None)
                if isinstance(nested, str) and nested:
                    declared = getattr(item, "profile_type", None)
                    refs[nested] = {
                        "json": "profile.json", "xml": "profile.xml",
                    }.get(declared, declared or "profile.json")
    symbols = SymbolTableV1(
        symbols=tuple(
            ComponentSymbolV1(ref=r, component_id="id" + r, component_type=t)
            for r, t in sorted(refs.items())
        )
    )

    # The gate itself is the subject. Driving the full compile would also
    # exercise emission planning, which needs richer connector symbols than this
    # test supplies and would fail for a reason that has nothing to do with the
    # gate — a green-for-the-wrong-reason risk in both directions.
    cfg = lower_process_ir_to_cfg(ir)
    compiler_pipeline._enforce_semantic_report(
        ir, cfg, symbols, None, None
    )  # must not raise


# ---------------------------------------------------------------------------
# Codex review round 8: three regressions introduced BY the relocation itself.
# ---------------------------------------------------------------------------


def _contracted_map_doc():
    """A flow that is valid ONLY because a typed map contract establishes DPP A."""
    return {
        "version": "1",
        "body": {
            "kind": "sequence",
            "steps": [
                {"kind": "source", "connection_ref": "$ref:conn",
                 "operation_ref": "$ref:op"},
                {"kind": "map_ref", "map_ref": "$ref:m"},
                {"kind": "set_dpp", "name": "OUT",
                 "source_values": [{"value_type": "dpp", "property_name": "A"}]},
                {"kind": "target", "connection_ref": "$ref:tconn",
                 "operation_ref": "$ref:top"},
                {"kind": "stop"},
            ],
        },
    }


def _contracted_symbols():
    from boomi_mcp.compiler.process_ir.contracts import (
        ComponentSymbolV1,
        SymbolTableV1,
    )

    return SymbolTableV1(symbols=tuple(
        ComponentSymbolV1(ref=ref, component_id="id" + ref, component_type=ctype)
        for ref, ctype in (
            ("$ref:conn", "connector-settings"),
            ("$ref:op", "connector-action"),
            ("$ref:tconn", "connector-settings"),
            ("$ref:top", "connector-action"),
            ("$ref:m", "transform.map"),
        )
    ))


def _map_capabilities():
    from boomi_mcp.compiler.process_ir.semantic_validation import (
        MapEffectContractV1,
        ProcessIRValidationCapabilitiesV1,
        StateEffectV1,
    )

    return ProcessIRValidationCapabilitiesV1(map_effects=(
        MapEffectContractV1(
            map_ref="$ref:m",
            effect=StateEffectV1(writes=(("dpp", "A"),), replay_safe=True),
        ),
    ))


def test_the_compiler_gate_accepts_a_trusted_capability_set():
    """Gating the compiler without threading capabilities through made the typed
    contract surface unusable for canonical compilation: `validate_process_ir`
    called with the same contracts reports the flow VALID, while the compiler
    rejected it because the gate always ran with the empty default."""
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    ir = parse_process_ir_v1(_contracted_map_doc())
    cfg = lower_process_ir_to_cfg(ir)
    compiler_pipeline._enforce_semantic_report(
        ir, cfg, _contracted_symbols(), None, _map_capabilities()
    )  # must not raise


def test_the_same_flow_without_the_contract_is_still_rejected():
    """The discriminator: capabilities must EARN the pass, not disable the gate."""
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.compiler.process_ir.lowering import lower_process_ir_to_cfg
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    ir = parse_process_ir_v1(_contracted_map_doc())
    cfg = lower_process_ir_to_cfg(ir)
    with pytest.raises(ProcessIRCompileError) as excinfo:
        compiler_pipeline._enforce_semantic_report(
            ir, cfg, _contracted_symbols(), None, None
        )
    assert "PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE" in [
        d.code for d in excinfo.value.diagnostics
    ]


def test_the_public_compiler_entry_point_accepts_capabilities():
    """The keyword has to exist on the API callers actually use, not only on the
    private helper."""
    import inspect

    parameters = inspect.signature(
        compiler_pipeline.compile_process_ir_v1
    ).parameters
    assert "capabilities" in parameters
    assert parameters["capabilities"].default is None


def test_an_unexpected_gate_failure_becomes_a_value_free_internal_diagnostic():
    """The gate runs through `_guarded`. Without that a collector defect escaped
    as a raw exception — and production builders catch `ProcessIRCompileError`,
    not arbitrary validator exceptions."""
    from boomi_mcp.compiler.process_ir import pipeline as cp
    from boomi_mcp.compiler.process_ir.diagnostics import ProcessIRCompileError
    from boomi_mcp.models.process_ir import parse_process_ir_v1

    def _boom(*_a, **_k):
        raise RuntimeError("authored-value-carrying text")

    ir = parse_process_ir_v1(_contracted_map_doc())
    original = cp._enforce_semantic_report
    cp._enforce_semantic_report = _boom
    try:
        with pytest.raises(ProcessIRCompileError) as excinfo:
            cp.compile_process_ir_v1(ir, _contracted_symbols())
    finally:
        cp._enforce_semantic_report = original
    codes = [d.code for d in excinfo.value.diagnostics]
    assert codes == ["PROCESS_IR_COMPILE_INTERNAL"]
    assert "authored-value-carrying" not in str(excinfo.value)


def test_the_bridge_does_not_chain_the_raw_validator_exception():
    """`from exc` kept the raw text in the traceback, defeating the redaction
    boundary for any caller or log that captures tracebacks. The compiler's own
    `_guarded` uses `from None` for exactly this reason."""
    import inspect

    from boomi_mcp.compiler.process_ir.semantic_validation import legacy_bridge

    source = inspect.getsource(legacy_bridge.validate_legacy_process_config)
    assert "from None" in source
    assert "from exc" not in source
