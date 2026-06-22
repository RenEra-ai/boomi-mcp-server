"""The five high-risk tools carry the gotcha hint only when the gotcha KB is on.

BOOMI_GOTCHAS_ENABLED is read once at server import, so the two flag states must
be checked in separate interpreters:
- DISABLED: the default test env (flag unset) — assert in-process that none of
  the five descriptions carry the hint marker.
- ENABLED: a fresh subprocess with BOOMI_GOTCHAS_ENABLED=true — assert all five
  gain the hint and an unrelated tool (manage_process) does not.
"""
import asyncio
import os
import subprocess
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402

# Distinctive substring of the gotcha hint appended by server._gotcha_hint.
HINT_MARKER = "field traps (a value silently dropped"

HINTED_TOOLS = (
    "build_integration",
    "orchestrate_deploy",
    "invoke_boomi_api",
    "manage_component",
    "manage_listeners",
)


def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _description(name):
    tool = _run_async(server.mcp.get_tool(name))
    return tool.description or ""


# ---------------------------------------------------------------------------
# Disabled (default test env): no hint on any of the five.
# ---------------------------------------------------------------------------

def test_hint_absent_when_gotchas_disabled():
    if server.BOOMI_GOTCHAS_ENABLED:
        import pytest
        pytest.skip("BOOMI_GOTCHAS_ENABLED is set in this environment")
    for name in HINTED_TOOLS:
        assert HINT_MARKER not in _description(name), (
            f"{name} must not carry the gotcha hint when the KB is disabled"
        )


# ---------------------------------------------------------------------------
# Enabled (fresh subprocess): all five gain the hint; an unrelated tool does not.
# ---------------------------------------------------------------------------

_ENABLED_SCRIPT = f"""
import asyncio
import server

MARKER = {HINT_MARKER!r}
HINTED = {list(HINTED_TOOLS)!r}

assert server.BOOMI_GOTCHAS_ENABLED is True

loop = asyncio.new_event_loop()
try:
    for name in HINTED:
        tool = loop.run_until_complete(server.mcp.get_tool(name))
        desc = tool.description or ""
        assert MARKER in desc, "missing gotcha hint on " + name

    # An unrelated tool must NOT pick up the hint.
    other = loop.run_until_complete(server.mcp.get_tool("manage_process"))
    assert MARKER not in (other.description or ""), "manage_process unexpectedly hinted"
finally:
    loop.close()

print("GOTCHA_HINT_ENABLED_OK")
"""


def test_hint_present_on_five_tools_when_enabled():
    env = os.environ.copy()
    env["BOOMI_LOCAL"] = "true"
    env["BOOMI_GOTCHAS_ENABLED"] = "true"
    env["BOOMI_DOCS_ENABLED"] = "false"
    result = subprocess.run(
        [sys.executable, "-c", _ENABLED_SCRIPT],
        cwd=str(_PROJECT_ROOT), env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "GOTCHA_HINT_ENABLED_OK" in result.stdout
