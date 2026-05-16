"""Wrapper tests for V3 archetype MCP tools (Issue #18).

Verifies that the server.py MCP wrappers:
- Register all three tools.
- Carry read-only / closed-world annotations.
- Pass through to the action layer with no credential / Boomi() calls.
"""

import asyncio
import os
import sys
from pathlib import Path
from unittest.mock import patch

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server (mirrors test_manage_deployment_wrapper.py)
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOOL_NAMES = (
    "list_integration_archetypes",
    "get_integration_archetype",
    "build_from_archetype",
)


def _annotation_value(annotations, key):
    """Return annotations[key] across attribute, dict, or Pydantic shapes."""
    if annotations is None:
        return None
    if hasattr(annotations, key):
        return getattr(annotations, key)
    if isinstance(annotations, dict):
        return annotations.get(key)
    if hasattr(annotations, "model_dump"):
        return annotations.model_dump().get(key)
    raise AssertionError(f"Cannot read annotation {key!r} from {annotations!r}")


def _resolve_tool(name):
    return asyncio.run(server.mcp.get_tool(name))


# ---------------------------------------------------------------------------
# Registration + annotations
# ---------------------------------------------------------------------------


def test_tools_registered():
    for name in TOOL_NAMES:
        tool = _resolve_tool(name)
        assert tool is not None, f"{name} not registered"


def test_tool_annotations_read_only_and_closed_world():
    for name in TOOL_NAMES:
        tool = _resolve_tool(name)
        ann = tool.annotations
        assert _annotation_value(ann, "readOnlyHint") is True, (
            f"{name} should have readOnlyHint=True"
        )
        assert _annotation_value(ann, "openWorldHint") is False, (
            f"{name} should have openWorldHint=False"
        )


# ---------------------------------------------------------------------------
# Direct wrapper invocation
# ---------------------------------------------------------------------------


def test_list_wrapper_returns_success():
    result = server.list_integration_archetypes()
    assert result["_success"] is True
    names = [a["name"] for a in result["archetypes"]]
    assert "stub_minimal_integration" in names


def test_get_wrapper_returns_success():
    result = server.get_integration_archetype("stub_minimal_integration")
    assert result["_success"] is True
    assert result["archetype"]["metadata"]["name"] == "stub_minimal_integration"


def test_build_wrapper_returns_success():
    result = server.build_from_archetype(
        "stub_minimal_integration",
        {"integration_name": "demo"},
    )
    assert result["_success"] is True
    assert result["integration_spec"]["name"] == "demo"
    assert result["boomi_mutation"] is False


# ---------------------------------------------------------------------------
# No credential or Boomi() calls
# ---------------------------------------------------------------------------


def test_wrappers_do_not_call_boomi_or_credentials():
    with (
        patch.object(server, "get_current_user") as m_user,
        patch.object(server, "get_secret") as m_secret,
        patch.object(server, "Boomi") as m_boomi,
    ):
        r1 = server.list_integration_archetypes()
        r2 = server.get_integration_archetype("stub_minimal_integration")
        r3 = server.build_from_archetype(
            "stub_minimal_integration",
            {"integration_name": "demo"},
        )

    assert r1["_success"] is True
    assert r2["_success"] is True
    assert r3["_success"] is True

    m_user.assert_not_called()
    m_secret.assert_not_called()
    m_boomi.assert_not_called()
