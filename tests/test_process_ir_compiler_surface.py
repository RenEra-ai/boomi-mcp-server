"""Issue #137 (M12.2): the compiler must be invisible to every public surface.

Acceptance criterion: "Internal CFG and emission-plan schemas are not present in
public MCP/LLM JSON Schema." The compiler exists so that callers CANNOT author
reachability, wiring, shape ids, or synthetic nodes — if any of those names
reached a tool schema, an LLM would start filling them in, and the boundary
would be gone.

The repo had no whole-surface scan before this: existing leak tests each pin a
hard-coded tool name. This module adds the missing primitive — iterate every
tool from ``server.mcp.list_tools()`` and scan its input schema, output schema,
and description together.

Names are matched EXACTLY, never as generic substrings: a token like "edge"
would false-positive on the unrelated public ``PipelineEdgeSpec``.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

_src = str(Path(__file__).resolve().parent.parent / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

# Must be set before ``import server`` (mirrors the other wrapper tests).
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402

import boomi_mcp.models as models  # noqa: E402
from boomi_mcp.models.process_ir import (  # noqa: E402
    canonical_process_ir_schema_json,
    process_ir_v1_json_schema,
)

# Distinctive compiler-internal identifiers. Every one is unique to the compiler
# — none is a generic word that could collide with a legitimate public name.
FORBIDDEN_NAMES = (
    "SemanticCfgV1",
    "CfgNodeV1",
    "CfgEdgeV1",
    "CfgSemanticV1",
    "CfgExitRoleV1",
    "CfgEdgeKindV1",
    "EmissionPlanV1",
    "EmissionNodeV1",
    "EmissionTransitionV1",
    "EmissionLayoutV1",
    "EmitterInputV1",
    "ComponentSymbolV1",
    "SymbolTableV1",
    # #142. ``IdempotencyContractSymbolV1`` is EXPORTED from the compiler package
    # (it is trusted compiler input, like ComponentSymbolV1 above) and is still
    # forbidden on the MCP surface — exported-to-the-compiler and visible-to-an-LLM
    # are different questions, and the second stays closed until #146.
    "IdempotencyContractSymbolV1",
    "IdempotencyEvidenceSemanticV1",
    "TryCatchSemanticV1",
    "CatchErrorsInputV1",
    "ConnectorCallBindingV1",
    "ErrorRegionV1",
    "retry_safety",
    "catch_region_node_ids",
    "CompilerDiagnostic",
    "ProcessIRCompileError",
    "exit_role",
    "synthetic_role",
    "emitter_input",
    "emitter_kind",
    "semantic_kind",
    "dragpoint_name",
    "cfg_node_id",
    "cfg_edge_id",
    "entry_node_id",
    "entry_shape_id",
    "terminal_shape_ids",
    "branch_leg",
    "decision_outcome",
    "routed_target",
    "start_noaction",
    "provenance_path",
    # #138 M12.3 process-emitter registry — dark, test-only, never a public surface.
    "emit_process",
    "EmitterRegistration",
    "EmitterContext",
    "ProcessEmissionArtifactV1",
    "ProcessVerifierSummaryV1",
    "SymbolRequirement",
    # #140 M12.5 ConnectorCall resolution — dark compiler internals. The AUTHORED
    # node kind ``connector_call`` is deliberately NOT here: it is part of the
    # internal ProcessIR schema (and pinned present below), unlike the binding
    # and capability machinery, which no caller may ever see.
    "ConnectorCallBindingV1",
    "ConnectorCallSemanticV1",
    "ConnectorCapabilityV1",
    "CONNECTOR_CALL_CAPABILITIES_V1",
    "resolve_connector_call_bindings",
    "validate_connector_call_semantics",
    "canonicalize_connector_metadata",
    "lookup_capability",
    "accepts_input",
    "produces_output",
    "connectoraction_source",
    "connectoraction_target",
)


def _run_async(coro):
    # A throwaway loop that is never registered as current: ``asyncio.run``
    # clears the thread's event loop on exit, which poisons legacy modules that
    # still call ``asyncio.get_event_loop()``.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _listed_tools():
    return _run_async(server.mcp.list_tools())


ALL_TOOLS = _listed_tools()


def _tool_surface(tool):
    """Everything about a tool an LLM can see: both schemas plus the description."""
    parts = [json.dumps(tool.parameters or {}, sort_keys=True)]
    output_schema = getattr(tool, "output_schema", None)
    if output_schema:
        parts.append(json.dumps(output_schema, sort_keys=True))
    parts.append(tool.description or "")
    parts.append(tool.name or "")
    return "\n".join(parts)


def test_the_tool_surface_scan_actually_sees_tools():
    """Guard the guard: an empty tool list would make every scan below vacuous."""
    assert len(ALL_TOOLS) > 10
    assert all(getattr(tool, "parameters", None) is not None for tool in ALL_TOOLS)


@pytest.mark.parametrize("forbidden", FORBIDDEN_NAMES)
def test_no_compiler_internal_appears_in_any_tool_schema(forbidden):
    """Whole-surface scan across EVERY registered MCP tool."""
    offenders = [
        tool.name for tool in ALL_TOOLS if forbidden in _tool_surface(tool)
    ]
    assert offenders == [], (
        "compiler-internal name {0!r} leaked into tool schema(s): {1}".format(
            forbidden, offenders
        )
    )


@pytest.mark.parametrize("forbidden", FORBIDDEN_NAMES)
def test_no_compiler_internal_appears_in_the_process_ir_schema(forbidden):
    assert forbidden not in canonical_process_ir_schema_json()
    assert forbidden not in json.dumps(process_ir_v1_json_schema(), sort_keys=True)


@pytest.mark.parametrize("forbidden", FORBIDDEN_NAMES)
def test_no_compiler_internal_appears_in_integration_spec_schema(forbidden):
    spec = getattr(models, "IntegrationSpecV1", None)
    if spec is None:  # pragma: no cover - defensive
        pytest.skip("IntegrationSpecV1 is not exported from boomi_mcp.models")
    assert forbidden not in json.dumps(spec.model_json_schema(), sort_keys=True)


def test_compiler_is_not_exported_from_boomi_mcp_models():
    exported = set(getattr(models, "__all__", ()))
    for forbidden in FORBIDDEN_NAMES:
        assert forbidden not in exported
    assert "compiler" not in exported


def test_importing_boomi_mcp_models_does_not_import_the_compiler():
    """The compiler must stay dark: nothing at runtime may pull it in.

    Checked in a FRESH subprocess — this process has already imported the
    compiler for the other tests, so an in-process ``sys.modules`` check would
    always pass and prove nothing.
    """
    import subprocess

    code = (
        "import sys; import boomi_mcp.models; "
        "mods=[m for m in sys.modules if 'boomi_mcp.compiler' in m]; "
        "print(mods)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=_src),
        cwd=_project_root,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[]", result.stdout


def test_importing_server_does_not_import_the_compiler():
    """No MCP tool path may reach the compiler while it is dark."""
    import subprocess

    # ``import server`` writes registration banners to stdout, so the result is
    # tagged and extracted rather than compared against the whole stream.
    code = (
        "import os; os.environ['BOOMI_LOCAL']='true'; "
        "import sys; import server; "
        "mods=[m for m in sys.modules if 'boomi_mcp.compiler' in m]; "
        "print('RESULT:' + repr(mods))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=_src),
        cwd=_project_root,
    )
    assert result.returncode == 0, result.stderr
    tagged = [
        line for line in result.stdout.splitlines() if line.startswith("RESULT:")
    ]
    assert tagged == ["RESULT:[]"], result.stdout


def test_compiler_package_is_importable_directly():
    """Dark does not mean broken — #138 imports it as a library."""
    from boomi_mcp.compiler.process_ir import compile_process_ir_v1

    assert callable(compile_process_ir_v1)


def test_connector_call_is_in_the_internal_ir_schema_but_no_public_surface():
    """#140's node kind is authored IR, so it MUST appear in the internal
    ProcessIR schema — and must still reach no MCP tool surface.

    **Scope amended by #153 (M12.15), deliberately and with the premise named.**
    The original clause also forbade ``connector_call`` anywhere in
    ``IntegrationSpecV1``'s JSON schema, on the stated grounds that "direct
    ProcessIR authoring is #146's to ship, not this issue's". #146 shipped it,
    and #153 then makes ProcessIR roots a FIRST-CLASS member of the spec
    (``IntegrationSpecV1.processes``, issue #153 in-scope item 4). So the spec
    schema now legitimately embeds the ProcessIR schema, and asserting its
    absence would assert against the contract this milestone exists to build.

    The limit is WITHDRAWN where it expired and RESTATED where it still bites,
    rather than reworded into something vacuous: ProcessIR may enter the spec
    only through ``processes``, and the LEGACY component surface must stay free
    of it. ``IntegrationComponentSpec`` is the shape every pre-#153 caller
    authors; if IR leaked into it, a component config would become a second,
    unpoliced place to author process semantics — exactly the dual-authority
    the milestone is consolidating away.
    """
    assert "connector_call" in canonical_process_ir_schema_json()
    offenders = [
        tool.name for tool in ALL_TOOLS if "connector_call" in _tool_surface(tool)
    ]
    assert offenders == [], offenders

    component = getattr(models, "IntegrationComponentSpec", None)
    if component is not None:
        assert "connector_call" not in json.dumps(
            component.model_json_schema(), sort_keys=True
        )

    # Positive control for the restated rule: the spec DOES carry the IR now,
    # and only by way of `processes`. Without this the assertion above could go
    # green because ProcessIR vanished from the spec entirely — which would mean
    # #153's roots were never wired in.
    spec = getattr(models, "IntegrationSpecV1", None)
    if spec is not None:
        assert "processes" in spec.model_fields
        assert "connector_call" in json.dumps(spec.model_json_schema(), sort_keys=True)


def test_connector_call_internals_stay_out_of_the_package_all():
    """The capability registry, the binding table and the resolver are imported
    directly by the pipeline and must never become a public surface."""
    from boomi_mcp.compiler.process_ir import __all__ as compiler_all

    for name in (
        "ConnectorCallBindingV1",
        "resolve_connector_call_bindings",
        "validate_connector_call_semantics",
        "validate_connector_calls",
        "CONNECTOR_CALL_CAPABILITIES_V1",
        "lookup_capability",
    ):
        assert name not in compiler_all


def test_process_emitter_registry_stays_out_of_the_package_all():
    """#138's registry is a TEST-ONLY consumer imported directly — it must not be
    re-exported through the compiler package's ``__all__`` (which would make it a
    public surface)."""
    from boomi_mcp.compiler.process_ir import __all__ as compiler_all

    for name in ("emit_process", "EmitterRegistration", "ProcessEmissionArtifactV1"):
        assert name not in compiler_all


def test_importing_the_compiler_package_does_not_eager_import_the_registry():
    """Importing the compiler package must not pull in the emitter registry (and
    thereby the graph verifier), keeping the dark package's import graph minimal."""
    import subprocess

    code = (
        "import sys; import boomi_mcp.compiler.process_ir; "
        "print('RESULT:' + str("
        "'boomi_mcp.compiler.process_ir.emitter_registry' in sys.modules))"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        env=dict(os.environ, PYTHONPATH=_src),
        cwd=_project_root,
    )
    assert result.returncode == 0, result.stderr
    assert "RESULT:False" in result.stdout, result.stdout


# ---------------------------------------------------------------------------
# Schema-name / template discovery (the OTHER public schema surface)
# ---------------------------------------------------------------------------


def _schema_names():
    from boomi_mcp.categories.meta_tools import _valid_schema_names

    return list(_valid_schema_names())


def test_schema_name_discovery_is_non_empty():
    """Guard the guard — an empty name list makes the scans below vacuous."""
    assert len(_schema_names()) > 0


@pytest.mark.parametrize("forbidden", FORBIDDEN_NAMES)
def test_no_compiler_internal_in_schema_name_discovery(forbidden):
    assert forbidden not in json.dumps(_schema_names(), sort_keys=True)


def test_no_compiler_internal_in_any_schema_template_payload():
    """Scan every discoverable schema template, not just the tool signatures.

    ``get_schema_template`` is a separate public surface from the MCP tool
    schemas: an LLM asks it for authoring templates, so a compiler-internal name
    appearing there would be just as authorable.
    """
    from boomi_mcp.categories.meta_tools import get_schema_template_action

    scanned = 0
    leaked = []
    for name in _schema_names():
        try:
            payload = get_schema_template_action(schema_name=name)
        except TypeError:
            # Not a schema_name-style template (different selector); skip.
            continue
        except Exception:  # pragma: no cover - discovery is best-effort
            continue
        blob = json.dumps(payload, sort_keys=True, default=str)
        scanned += 1
        for forbidden in FORBIDDEN_NAMES:
            if forbidden in blob:
                leaked.append((name, forbidden))
    assert scanned > 0, "no schema template was actually scanned — test is vacuous"
    assert leaked == [], leaked
