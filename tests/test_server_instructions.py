"""Tests for the always-on FastMCP server instructions (Issue #10).

The default test environment has BOOMI_DOCS_ENABLED unset, so SERVER_INSTRUCTIONS
must equal the authoring doctrine alone (no KB text appended).
"""

import os
import sys
from pathlib import Path

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Force local mode before importing server.
os.environ["BOOMI_LOCAL"] = "true"

import server  # noqa: E402


def test_server_instructions_always_on():
    assert isinstance(server.SERVER_INSTRUCTIONS, str)
    assert server.SERVER_INSTRUCTIONS.strip()
    assert server.mcp.instructions == server.SERVER_INSTRUCTIONS


def test_instructions_route_profile_and_archetype_first():
    text = server.SERVER_INSTRUCTIONS
    assert "list_boomi_profiles" in text
    assert "list_integration_archetypes" in text
    assert "build_from_archetype" in text
    assert "list_capabilities" in text
    assert "get_schema_template" in text


def test_instructions_state_enforced_confirm_write():
    text = server.SERVER_INSTRUCTIONS
    assert "confirm_write" in text
    assert "enforced" in text.lower()


def test_instructions_carry_companion_unverified_auth_stop():
    assert "[companion_unverified]" in server.SERVER_INSTRUCTIONS


def test_instructions_exclude_kb_text_when_docs_disabled():
    # Default test env: BOOMI_DOCS_ENABLED unset → doctrine only.
    if server.BOOMI_DOCS_ENABLED:
        import pytest
        pytest.skip("BOOMI_DOCS_ENABLED is set in this environment")
    assert server.SERVER_INSTRUCTIONS == server.AGENT_AUTHORING_INSTRUCTIONS
    assert "documentation retrieval" not in server.SERVER_INSTRUCTIONS
