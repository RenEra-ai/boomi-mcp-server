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
