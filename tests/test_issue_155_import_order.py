"""Every served module must import cleanly as the FIRST module imported.

A circular import shipped in this slice and no test saw it. The reason is
structural rather than bad luck: a test suite imports its subject through
conftest and fixtures, so by the time any module under test is loaded, the
low-level modules it depends on are already initialised. The cycle only appears
when the higher-layer module is the entry point — which is exactly what happens
to an operator importing a module directly, and to any future tool whose import
graph differs from the suite's.

So this runs each module as the first import in a FRESH interpreter. Anything
that builds served state at import time and reaches back up a layer to do it
fails here and nowhere else.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

#: The modules whose import-time work is served. `errors` builds the taxonomy at
#: import; the authoring modules are what a caller reaches first.
ENTRY_POINTS = [
    "boomi_mcp.errors",
    "boomi_mcp.authoring.connector_resolution_snapshot",
    "boomi_mcp.authoring.workflow",
    "boomi_mcp.categories.integration_builder",
    "boomi_mcp.recipes.materialization",
    "boomi_mcp.compiler.process_ir.connector_resolution",
]


@pytest.mark.parametrize("module", ENTRY_POINTS)
def test_the_module_imports_first_without_a_cycle(module):
    """Import it as the first `boomi_mcp` module, then use the served taxonomy.

    Importing alone is not enough: the failure mode is a partially initialised
    module satisfying an import and failing later, so this also touches the
    served state the import was supposed to build.
    """
    program = (
        f"import {module}\n"
        "from boomi_mcp.errors import ERROR_TAXONOMY\n"
        "assert ERROR_TAXONOMY, 'the served taxonomy is empty'\n"
        "spec = ERROR_TAXONOMY['CONNECTOR_REPLAY_SUBMITTED_XML_UNREADABLE']\n"
        "assert spec.summary.strip(), 'the served summary is empty'\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={
            "PYTHONPATH": str(SRC),
            "PYTHONDONTWRITEBYTECODE": "1",
            "BOOMI_LOCAL": "true",
            "PATH": "/usr/bin:/bin",
        },
    )
    assert result.returncode == 0, (
        f"{module} cannot be imported first:\n{result.stderr[-1500:]}"
    )


def test_the_guard_would_catch_a_cycle():
    """Non-vacuity: a suite that always imports the low layer first sees nothing.

    This plants the shape of the defect — a module reaching back into a
    higher layer during ITS OWN import — and asserts the harness above reports
    it. Without this, the parametrized test passes for a codebase with no
    modules at all.
    """
    program = (
        "import sys, types\n"
        "a = types.ModuleType('cycle_a')\n"
        "b = types.ModuleType('cycle_b')\n"
        "sys.modules['cycle_a'] = a\n"
        "sys.modules['cycle_b'] = b\n"
        "exec('from cycle_b import missing_name', a.__dict__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=ROOT, capture_output=True, text=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert result.returncode != 0, "the harness cannot observe an import failure"
    assert "ImportError" in result.stderr or "cannot import name" in result.stderr
