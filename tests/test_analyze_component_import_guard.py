"""Regression guard for the analyze_component build-context exclusion bug.

An unanchored ``analyze_*.py`` rule in .gitignore plus a missing .gcloudignore caused
local ``gcloud builds submit`` uploads to drop the tracked module
``src/boomi_mcp/categories/components/analyze_component.py``. Because
``components/__init__.py`` eagerly imported it, the missing file disabled FOUR tool
categories at startup with "No module named ...analyze_component". These tests assert:

  1. each required tool module imports and exposes a callable action,
  2. the lazy ``components/__init__`` no longer cascades one broken submodule into
     siblings (so a future breakage can affect at most its own category), and
  3. the server registers the four high-value MCP tools.
"""

import asyncio
import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Force local mode before importing server (mirrors tests/test_list_capabilities_wrapper.py).
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402  (importing server adds src/ to sys.path)


def _run_async(coro):
    # Throwaway loop never registered as current — mirrors test_list_capabilities_wrapper.
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# (module path, action attribute) for each of the four categories the bug disabled.
REQUIRED_TOOL_MODULES = [
    ("boomi_mcp.categories.components.analyze_component", "analyze_component_action"),
    ("boomi_mcp.categories.components.trading_partners", "manage_trading_partner_action"),
    ("boomi_mcp.categories.components.connectors", "manage_connector_action"),
    ("boomi_mcp.categories.integration_builder", "build_integration_action"),
]

# MCP tool names the four modules register.
REQUIRED_TOOL_NAMES = [
    "analyze_component",
    "manage_trading_partner",
    "manage_connector",
    "build_integration",
]


@pytest.mark.parametrize("module_path, action_attr", REQUIRED_TOOL_MODULES)
def test_required_tool_module_imports_and_action_callable(module_path, action_attr):
    module = importlib.import_module(module_path)
    action = getattr(module, action_attr, None)
    assert callable(action), f"{module_path}.{action_attr} must import as a callable"


def test_lazy_components_init_does_not_cascade():
    """Importing one components submodule must NOT eagerly import analyze_component.

    Run in a fresh interpreter so the assertion is not masked by modules already
    imported elsewhere in this test session.
    """
    code = (
        "import sys, importlib\n"
        "importlib.import_module('boomi_mcp.categories.components.connectors')\n"
        "assert 'boomi_mcp.categories.components.analyze_component' not in sys.modules, "
        "'importing connectors must not pull analyze_component (cascade isolation)'\n"
    )
    env = {**os.environ, "PYTHONPATH": str(_PROJECT_ROOT / "src"), "BOOMI_LOCAL": "true"}
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        cwd=str(_PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"stdout={result.stdout!r} stderr={result.stderr!r}"


def test_server_registers_all_four_high_value_tools():
    registered = {t.name for t in _run_async(server.mcp.list_tools())}
    missing = [name for name in REQUIRED_TOOL_NAMES if name not in registered]
    assert not missing, f"server did not register required tools: {missing!r}"
