"""Tests for list_capabilities catalog accuracy against live FastMCP registry."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Force local mode so all tools (including credential tools) are registered
os.environ["BOOMI_LOCAL"] = "true"

import server
from boomi_mcp.categories.meta_tools import list_capabilities_action


# ── Helpers ──────────────────────────────────────────────────────────

def _get_capabilities():
    """Call list_capabilities via the registered FunctionTool."""
    tool = server.mcp._tool_manager._tools["list_capabilities"]
    return tool.fn()


def _get_registry_names():
    """Return the set of tool names from the live FastMCP registry."""
    return set(server.mcp._tool_manager._tools.keys())


# ── Tests ────────────────────────────────────────────────────────────

class TestCatalogMatchesRegistry:
    """Ensure list_capabilities catalog is in sync with registered MCP tools."""

    def test_catalog_matches_registry(self):
        result = _get_capabilities()
        assert result["_success"] is True
        catalog_tools = set(result["tools"].keys())
        registry_tools = _get_registry_names()
        assert catalog_tools == registry_tools, (
            f"Catalog drift detected.\n"
            f"  In registry but not catalog: {registry_tools - catalog_tools}\n"
            f"  In catalog but not registry: {catalog_tools - registry_tools}"
        )

    def test_counts_consistent(self):
        result = _get_capabilities()
        tools = result["tools"]
        assert result["total_tools"] == len(tools)
        assert result["implemented_count"] + result["not_implemented_count"] == result["total_tools"]
        assert set(result["implemented_tools"]) | set(result["not_implemented_tools"]) == set(tools.keys())


class TestMissingToolsPresent:
    """Verify the four previously-missing tools now appear in local mode."""

    @pytest.mark.parametrize("tool_name", [
        "manage_shared_resources",
        "manage_account",
        "set_boomi_credentials",
        "delete_boomi_profile",
    ])
    def test_tool_present(self, tool_name):
        result = _get_capabilities()
        assert tool_name in result["tools"], f"{tool_name} missing from catalog"


class TestActionLists:
    """Verify action lists include previously-omitted entries."""

    def test_environment_actions_complete(self):
        result = _get_capabilities()
        actions = result["tools"]["manage_environments"]["actions"]
        assert "get_properties" in actions
        assert "update_properties" in actions

    def test_runtimes_diagnostics_present(self):
        result = _get_capabilities()
        actions = result["tools"]["manage_runtimes"]["actions"]
        assert "diagnostics" in actions

    def test_execute_process_implemented(self):
        result = _get_capabilities()
        ep = result["tools"]["execute_process"]
        assert ep.get("implemented", True) is True
        assert "execute_process" not in result["not_implemented_tools"]
        assert "execute_process" in result["implemented_tools"]


class TestFilteredHelper:
    """Test the available_tools filtering at the helper level."""

    def test_excludes_local_only_tools(self):
        """A production-like subset should exclude credential management tools."""
        # Build a subset that mirrors production (no set_boomi_credentials / delete_boomi_profile)
        prod_tools = _get_registry_names() - {"set_boomi_credentials", "delete_boomi_profile"}
        result = list_capabilities_action(available_tools=prod_tools)
        assert result["_success"] is True
        assert "set_boomi_credentials" not in result["tools"]
        assert "delete_boomi_profile" not in result["tools"]
        # Core tools should still be present
        assert "query_components" in result["tools"]
        assert "manage_environments" in result["tools"]

    def test_no_filter_returns_all(self):
        """Calling without available_tools returns full catalog (backwards compat)."""
        result = list_capabilities_action()
        assert result["_success"] is True
        # Should include all entries including local-only ones
        assert "set_boomi_credentials" in result["tools"]
        assert "delete_boomi_profile" in result["tools"]


class TestWorkflowMetadata:
    """Verify workflow guidance is up to date."""

    def test_workflow_uses_execute_process(self):
        result = _get_capabilities()
        steps = result["workflows"]["create_and_deploy_process"]["steps"]
        # Step 5 should reference execute_process, not invoke_boomi_api
        step_5 = steps[4]
        assert "execute_process" in step_5
        assert "invoke_boomi_api" not in step_5

    def test_admin_workflow_uses_manage_account(self):
        result = _get_capabilities()
        steps = result["workflows"]["manage_admin_operations"]["steps"]
        role_steps = [s for s in steps if "role" in s.lower()]
        branch_steps = [s for s in steps if "branch" in s.lower()]
        # Roles and branches should route through manage_account
        for s in role_steps + branch_steps:
            assert "manage_account" in s, f"Expected manage_account in: {s}"
            assert "invoke_boomi_api" not in s, f"Stale invoke_boomi_api ref in: {s}"

    def test_filtered_workflows_exclude_absent_tools(self):
        """Workflows referencing tools not in available_tools should be dropped."""
        # Use a subset that excludes manage_account
        subset = _get_registry_names() - {"manage_account"}
        result = list_capabilities_action(available_tools=subset)
        # manage_admin_operations references manage_account, so it should be gone
        assert "manage_admin_operations" not in result["workflows"]
        # Workflows that only reference present tools should remain
        assert "discover_components" in result["workflows"]
