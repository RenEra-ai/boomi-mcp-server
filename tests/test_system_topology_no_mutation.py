"""Behavioral proof that no mutating or executing tool is reachable (#144, M12.9).

The acceptance criterion is "no mutating/executing MCP tool is reachable from
this issue's topology planner". A grep over imports is a weak proof of that: it
catches the obvious spelling and nothing else. These tests prove it by RUNNING
the planner with the operations in question made impossible.

Two techniques, both necessarily in subprocesses:

* ``sys.addaudithook`` cannot be removed once installed, and pytest itself opens
  files for writing (capture, cacheprovider, ``__pycache__``), so installing one
  in this process would poison the whole run.
* An import ledger only means something in an interpreter that has not already
  imported everything for the other tests.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
_src = str(_project_root / "src")
if _src not in sys.path:
    sys.path.insert(0, _src)

from boomi_mcp.compiler.system_topology import plan_system_topology
from boomi_mcp.compiler.system_topology.context import (
    ComponentPlanSymbolV1,
    TopologyResolutionContextV1,
)
from boomi_mcp.models.system_topology import parse_system_topology_v1

_TOPOLOGY_PKG = _project_root / "src" / "boomi_mcp" / "compiler" / "system_topology"

_SPEC_LITERAL = """{
    "version": "1",
    "profile_ref": "prof",
    "objects": [
        {"kind": "process", "key": "a", "component_ref": "$ref:ka"},
        {"kind": "process", "key": "b", "component_ref": "$ref:kb"}
    ],
    "relations": [
        {"kind": "process_call", "key": "r", "caller_process": "a", "callee_process": "b"}
    ]
}"""


def _run(code, env_extra=None):
    env = dict(os.environ, PYTHONPATH=_src, PYTHONDONTWRITEBYTECODE="1")
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, "-B", "-c", code],
        capture_output=True,
        text=True,
        env=env,
        cwd=_project_root,
    )


def _spec():
    import json

    return parse_system_topology_v1(json.loads(_SPEC_LITERAL))


def _context():
    return TopologyResolutionContextV1(
        profile="prof",
        component_plan_symbols=(
            ComponentPlanSymbolV1(component_key="ka", component_type="process"),
            ComponentPlanSymbolV1(component_key="kb", component_type="process"),
        ),
    )


# ---------------------------------------------------------------------------
# Import ledger
# ---------------------------------------------------------------------------

_FORBIDDEN_MODULE_MARKERS = (
    "server",
    "boomi_sdk",
    "boomi.",
    "requests",
    "httpx",
    "urllib3",
    "boomi_mcp.categories.execution",
    "boomi_mcp.categories.deployment",
    "boomi_mcp.categories.schedules",
    "boomi_mcp.categories.environments",
    "boomi_mcp.categories.listeners",
    "boomi_mcp.categories.shared_resources",
    "boomi_mcp.categories.troubleshooting",
)


def test_planning_imports_no_mutating_or_networked_module():
    """Runs a real plan in a fresh interpreter and inspects the module ledger."""
    code = f"""
import json, sys
from boomi_mcp.models.system_topology import parse_system_topology_v1
from boomi_mcp.compiler.system_topology import plan_system_topology
from boomi_mcp.compiler.system_topology.context import (
    ComponentPlanSymbolV1, TopologyResolutionContextV1,
)
spec = parse_system_topology_v1(json.loads({_SPEC_LITERAL!r}))
ctx = TopologyResolutionContextV1(
    profile="prof",
    component_plan_symbols=(
        ComponentPlanSymbolV1(component_key="ka", component_type="process"),
        ComponentPlanSymbolV1(component_key="kb", component_type="process"),
    ),
)
plan = plan_system_topology(spec, ctx)
assert plan.apply_supported is False
markers = {_FORBIDDEN_MODULE_MARKERS!r}
hits = sorted({{m for m in sys.modules for k in markers if k in m}})
print("RESULT:" + repr(hits))
"""
    result = _run(code)
    assert result.returncode == 0, result.stderr
    line = [l for l in result.stdout.splitlines() if l.startswith("RESULT:")][0]
    assert line == "RESULT:[]", line


def test_the_import_ledger_would_notice_a_real_import():
    """Positive control: a ledger that matches nothing passes vacuously."""
    code = f"""
import sys, urllib3  # noqa: F401
markers = {_FORBIDDEN_MODULE_MARKERS!r}
hits = sorted({{m for m in sys.modules for k in markers if k in m}})
print("RESULT:" + repr(bool(hits)))
"""
    result = _run(code)
    assert result.returncode == 0, result.stderr
    assert "RESULT:True" in result.stdout, result.stdout


# ---------------------------------------------------------------------------
# Audit hook
# ---------------------------------------------------------------------------

_AUDIT_HOOK = '''
import json, sys

# Import EVERYTHING first. The hook cannot be removed once installed, and import
# machinery legitimately opens files — auditing it would prove nothing about the
# planner and would abort the run before it started.
from boomi_mcp.models.system_topology import parse_system_topology_v1
from boomi_mcp.compiler.system_topology import plan_system_topology
from boomi_mcp.compiler.system_topology.context import (
    ComponentPlanSymbolV1, ProcessCallEvidenceV1, TopologyResolutionContextV1,
)
import boomi_mcp.compiler.system_topology.discovery  # noqa: F401
import boomi_mcp.compiler.system_topology.evidence   # noqa: F401

spec = parse_system_topology_v1(json.loads(SPEC))
# Witnessed, so the plan is genuinely CLEAN under the hook. A gated plan would
# short-circuit before the assembly, ordering and invariant code ever ran —
# proving the hook holds for a code path the planner barely entered.
ctx = TopologyResolutionContextV1(
    profile="prof",
    component_plan_symbols=(
        ComponentPlanSymbolV1(component_key="ka", component_type="process"),
        ComponentPlanSymbolV1(component_key="kb", component_type="process"),
    ),
    process_call_evidence=(
        ProcessCallEvidenceV1(
            caller_component_ref="$ref:ka",
            callee_component_ref="$ref:kb",
            witness="process_ir",
        ),
    ),
)

class Tripwire(Exception):
    pass

TRIPPED = []

_WRITE_MODES = set("wax+")

def hook(event, args):
    if event == "open":
        # The ``open`` audit event fires for READS too. Rejecting it
        # unconditionally would make this test "pass" while proving nothing,
        # because the planner would never get to run at all.
        mode = args[1] if len(args) > 1 else None
        if isinstance(mode, str) and _WRITE_MODES & set(mode):
            TRIPPED.append(("open", mode))
            raise Tripwire("write open")
        return
    if event in (
        "socket.__new__", "socket.connect", "socket.bind",
        "subprocess.Popen", "os.system", "os.exec", "os.spawn",
        "os.putenv", "os.unsetenv", "os.remove", "os.rename",
        "os.mkdir", "os.rmdir", "os.chmod", "shutil.copyfile",
        "shutil.move", "shutil.rmtree", "urllib.Request",
    ):
        TRIPPED.append((event, None))
        raise Tripwire(event)

sys.addaudithook(hook)

plan = plan_system_topology(spec, ctx)
print("PLANNED:" + repr(plan.apply_supported is False and len(plan.blockers) == 0))
print("TRIPPED_DURING_PLAN:" + repr(list(TRIPPED)))

# Positive control, in the SAME process and under the SAME hook. Without it, a
# hook that silently never fires is indistinguishable from a clean plan.
control = False
try:
    import socket
    socket.socket()
except Tripwire:
    control = True
except Exception:
    control = False
print("CONTROL_FIRED:" + repr(control))
'''


def test_planning_performs_no_write_network_or_process_operation():
    code = f"SPEC = {_SPEC_LITERAL!r}\n" + _AUDIT_HOOK
    result = _run(code)
    assert result.returncode == 0, result.stderr + result.stdout
    out = result.stdout
    assert "PLANNED:True" in out, out
    assert "TRIPPED_DURING_PLAN:[]" in out, out
    # The hook is real: it fired on the deliberate violation.
    assert "CONTROL_FIRED:True" in out, out


# ---------------------------------------------------------------------------
# Context shape — nothing executable can even be handed in
# ---------------------------------------------------------------------------


def test_no_context_or_plan_model_declares_an_open_or_callable_field():
    """A planner that cannot be GIVEN a client cannot call one.

    Walking the annotations rather than the values: a field typed ``Any`` is a
    hole regardless of what any particular caller puts in it.
    """
    import typing

    from boomi_mcp.compiler.system_topology import context as ctx_mod
    from boomi_mcp.compiler.system_topology import contracts as contracts_mod

    forbidden_names = {
        "Any",
        "Callable",
        "object",
        "Dict",
        "dict",
    }
    checked = 0
    for module in (ctx_mod, contracts_mod):
        for name in dir(module):
            model = getattr(module, name)
            fields = getattr(model, "model_fields", None)
            if not isinstance(fields, dict):
                continue
            # Only models THIS package defines. ``context`` imports
            # ``IntegrationSpecV1`` to project from it, and that model's open
            # ``config``/``endpoints`` fields are exactly what the projection
            # exists to strip — auditing it here would assert the wrong thing.
            if not getattr(model, "__module__", "").startswith(
                "boomi_mcp.compiler.system_topology"
            ):
                continue
            checked += 1
            for field_name, field in fields.items():
                rendered = str(field.annotation)
                for bad in forbidden_names:
                    assert f"{bad}]" not in rendered and rendered != bad, (
                        name,
                        field_name,
                        rendered,
                    )
                assert "Callable" not in rendered, (name, field_name)
    assert checked >= 15, checked


def test_no_context_field_is_named_for_an_action_or_payload():
    from boomi_mcp.compiler.system_topology import context as ctx_mod

    forbidden = {
        "action",
        "config",
        "xml",
        "raw_xml",
        "client",
        "session",
        "sdk",
        "port",
        "credentials",
        "password",
        "token",
        "confirm_write",
    }
    for name in dir(ctx_mod):
        model = getattr(ctx_mod, name)
        fields = getattr(model, "model_fields", None)
        if not isinstance(fields, dict):
            continue
        if not getattr(model, "__module__", "").startswith(
            "boomi_mcp.compiler.system_topology"
        ):
            continue
        assert forbidden.isdisjoint(set(fields)), (name, sorted(set(fields) & forbidden))


def test_a_plan_payload_carries_no_executable_vocabulary():
    plan = plan_system_topology(_spec(), _context())
    from boomi_mcp.compiler.system_topology import canonical_topology_plan_json

    blob = canonical_topology_plan_json(plan)
    for forbidden in (
        '"action"',
        '"confirm_write"',
        '"dry_run"',
        '"deploy"',
        '"execute"',
        '"config"',
        '"xml"',
        '"password"',
    ):
        assert forbidden not in blob, forbidden


def test_the_invariant_check_rejects_a_plan_carrying_an_action():
    """Positive control for the invariant, driven through its own scanner."""
    from boomi_mcp.compiler.system_topology.invariants import _find_forbidden

    assert _find_forbidden({"nested": [{"action": "deploy"}]}) is True
    assert _find_forbidden({"nested": [{"relation_kind": "process_call"}]}) is False


# ---------------------------------------------------------------------------
# No MCP surface
# ---------------------------------------------------------------------------


def test_no_topology_tool_is_registered_on_the_mcp_surface():
    """#144 ships dark; #146 owns the eventual planning surface."""
    import asyncio

    os.environ.setdefault("BOOMI_LOCAL", "true")
    sys.path.insert(0, str(_project_root))
    import server

    tools = asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
        server.mcp.list_tools()
    )
    names = {getattr(tool, "name", "") for tool in tools}
    for name in names:
        assert "topology" not in name.lower(), name


def test_importing_the_server_does_not_import_the_topology_planner():
    """The PLANNER stays dark.

    ``boomi_mcp.models.system_topology`` is deliberately NOT in scope here: the
    authored model is re-exported through ``boomi_mcp.models`` by design, the
    same way ``ProcessIRV1`` is. What must not load is
    ``boomi_mcp.compiler.system_topology`` — the planner.
    """
    code = (
        "import os; os.environ['BOOMI_LOCAL']='true'; "
        "import sys; import server; "
        "mods=[m for m in sys.modules if 'compiler.system_topology' in m]; "
        "print('RESULT:' + repr(mods))"
    )
    result = _run(code)
    assert result.returncode == 0, result.stderr
    tagged = [l for l in result.stdout.splitlines() if l.startswith("RESULT:")]
    assert tagged == ["RESULT:[]"], result.stdout


def test_importing_boomi_mcp_models_does_not_import_the_topology_planner():
    code = (
        "import sys; import boomi_mcp.models; "
        "mods=[m for m in sys.modules if 'compiler.system_topology' in m]; "
        "print('RESULT:' + repr(mods))"
    )
    result = _run(code)
    assert result.returncode == 0, result.stderr
    assert "RESULT:[]" in result.stdout, result.stdout


def _code_lines(source):
    """Source lines with string literals and comments removed.

    A blunt substring scan over raw source flags
    ``"capture:manage_deployment/records-read-only"`` — an evidence-provenance
    TOKEN recording which live read produced a fact. That string is the honest
    citation the capability registry is built on; a test that forbade it would
    push the registry toward unattributed claims, which is the opposite of what
    this issue is for. So string contents are stripped before scanning.
    """
    import io
    import tokenize

    kept = []
    try:
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type in (tokenize.STRING, tokenize.COMMENT):
                continue
            kept.append(token.string)
    except tokenize.TokenError:  # pragma: no cover - source is always valid here
        return source
    return " ".join(kept)


def test_no_topology_source_file_calls_a_mutating_mcp_tool():
    """Belt to the audit hook's braces — cheap, and it localizes a regression."""
    mutating_tools = (
        "build_integration",
        "build_from_archetype",
        "orchestrate_deploy",
        "execute_process",
        "manage_component",
        "manage_deployment",
        "manage_schedules",
        "manage_listeners",
        "manage_runtimes",
        "invoke_boomi_api",
        "apply_component_edit",
    )
    for path in sorted(_TOPOLOGY_PKG.glob("*.py")):
        code = _code_lines(path.read_text())
        for tool in mutating_tools:
            assert tool not in code, (path.name, tool)


def test_the_source_scan_still_sees_real_code():
    """Positive control: stripping strings must not strip everything."""
    stripped = _code_lines('x = "orchestrate_deploy"\ndef orchestrate_deploy(): pass\n')
    assert "def" in stripped
    assert stripped.count("orchestrate_deploy") == 1


@pytest.mark.parametrize("operation", ["apply", "deploy", "execute", "schedule", ""])
def test_every_non_plan_operation_is_refused(operation):
    plan = plan_system_topology(_spec(), _context(), operation)
    assert [b.code for b in plan.blockers] == ["TOPOLOGY_APPLY_NOT_SUPPORTED"]
    assert plan.apply_supported is False
