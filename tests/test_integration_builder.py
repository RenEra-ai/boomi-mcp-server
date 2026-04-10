"""Regression tests for integration-builder collision safety.

Verifies that _resolve_existing_components, _build_plan, and _apply_plan
handle ambiguous same-name matches correctly instead of silently picking one.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.boomi_mcp.models.integration_models import IntegrationComponentSpec
from src.boomi_mcp.categories.integration_builder import (
    _resolve_existing_components,
    _build_plan,
    _apply_plan,
)

_PATCH_TARGET = "src.boomi_mcp.categories.integration_builder.paginate_metadata"


def _meta(component_id, name, folder_name="Root", comp_type="process"):
    """Build a metadata dict matching the shape returned by paginate_metadata."""
    return {
        "component_id": component_id,
        "id": component_id,
        "name": name,
        "folder_name": folder_name,
        "type": comp_type,
        "version": 1,
        "current_version": True,
        "deleted": False,
        "created_date": "",
        "modified_date": "",
        "created_by": "",
        "modified_by": "",
    }


def _comp(key="p1", name="MyProcess", action="create", comp_type="process",
          component_id=None):
    return IntegrationComponentSpec(
        key=key, type=comp_type, action=action, name=name,
        component_id=component_id, config={"name": name} if comp_type == "process" else {},
    )


def _build_config(components, conflict_policy="reuse"):
    """Minimal config dict accepted by _build_plan."""
    return {
        "conflict_policy": conflict_policy,
        "integration_spec": {
            "version": "1.0",
            "name": "test-integration",
            "components": [
                c.model_dump() if hasattr(c, "model_dump") else c
                for c in components
            ],
        },
    }


# ---------------------------------------------------------------------------
# _resolve_existing_components
# ---------------------------------------------------------------------------
class TestResolveExistingComponents:

    @patch(_PATCH_TARGET)
    def test_returns_all_matching_dicts(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-1", "MyProcess", "FolderA"),
            _meta("id-2", "MyProcess", "FolderB"),
            _meta("id-3", "OtherProcess", "FolderA"),
        ]
        comp = _comp(name="MyProcess")
        result = _resolve_existing_components(MagicMock(), comp)
        assert len(result) == 2
        ids = [r["component_id"] for r in result]
        assert "id-1" in ids
        assert "id-2" in ids

    def test_returns_empty_when_no_name(self):
        comp = _comp(name=None)
        result = _resolve_existing_components(MagicMock(), comp)
        assert result == []

    @patch(_PATCH_TARGET)
    def test_returns_empty_when_no_metadata_type(self, mock_pag):
        comp = _comp(comp_type="unknown_thing")
        result = _resolve_existing_components(MagicMock(), comp)
        assert result == []
        mock_pag.assert_not_called()


# ---------------------------------------------------------------------------
# _build_plan – ambiguity detection
# ---------------------------------------------------------------------------
class TestBuildPlanAmbiguity:

    @patch(_PATCH_TARGET)
    def test_reuse_two_matches_reports_ambiguous(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-1", "Dup", "FolderA"),
            _meta("id-2", "Dup", "FolderB"),
        ]
        config = _build_config([_comp(name="Dup")], conflict_policy="reuse")
        plan = _build_plan(MagicMock(), config)
        assert plan["_success"] is True
        step = plan["steps"][0]
        assert step["planned_action"] == "error_ambiguous_match"
        assert len(step["candidates"]) == 2

    @patch(_PATCH_TARGET)
    def test_clone_two_matches_still_creates_clone(self, mock_pag):
        """Clone creates a new component with a suffix — no targeting risk even
        when multiple same-name matches exist."""
        mock_pag.return_value = [
            _meta("id-1", "Dup", "FolderA"),
            _meta("id-2", "Dup", "FolderB"),
        ]
        config = _build_config([_comp(name="Dup")], conflict_policy="clone")
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "create_clone"
        # existing_component_id must be set so _apply_plan enters the clone-suffix branch
        assert step["existing_component_id"] is not None

    @patch(_PATCH_TARGET)
    def test_update_no_id_two_matches_reports_ambiguous(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-1", "Dup", "FolderA"),
            _meta("id-2", "Dup", "FolderB"),
        ]
        config = _build_config(
            [_comp(name="Dup", action="update")], conflict_policy="reuse"
        )
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_ambiguous_match"

    @patch(_PATCH_TARGET)
    def test_update_no_id_zero_matches_reports_missing(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config(
            [_comp(name="Ghost", action="update")], conflict_policy="reuse"
        )
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_missing_target"

    @patch(_PATCH_TARGET)
    def test_single_match_reuse_preserves_behavior(self, mock_pag):
        mock_pag.return_value = [_meta("id-1", "Solo", "Root")]
        config = _build_config([_comp(name="Solo")], conflict_policy="reuse")
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "reuse"
        assert step["existing_component_id"] == "id-1"

    @patch(_PATCH_TARGET)
    def test_explicit_component_id_skips_ambiguity(self, mock_pag):
        config = _build_config(
            [_comp(name="X", action="update", component_id="explicit-id")],
            conflict_policy="reuse",
        )
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["existing_component_id"] == "explicit-id"
        assert step["planned_action"] == "update"
        mock_pag.assert_not_called()


# ---------------------------------------------------------------------------
# _apply_plan – fail-fast on ambiguity
# ---------------------------------------------------------------------------
class TestApplyPlanAmbiguity:

    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_ambiguous(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-1", "Dup", "A"),
            _meta("id-2", "Dup", "B"),
        ]
        config = _build_config([_comp(name="Dup")], conflict_policy="reuse")
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert "unresolvable_steps" in result
        assert len(result["unresolvable_steps"]) == 1
        assert result["unresolvable_steps"][0]["planned_action"] == "error_ambiguous_match"

    @patch(_PATCH_TARGET)
    def test_dry_run_shows_ambiguous_planned_action(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-1", "Dup", "A"),
            _meta("id-2", "Dup", "B"),
        ]
        config = _build_config([_comp(name="Dup")], conflict_policy="reuse")
        config["dry_run"] = True
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["dry_run"] is True
        step = result["steps"][0]
        assert step["planned_action"] == "error_ambiguous_match"

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_clone_multi_match_applies_suffix(self, mock_pag, mock_exec):
        """Clone with multiple matches must reach _apply_clone_suffix, not skip it."""
        mock_pag.return_value = [
            _meta("id-1", "Dup", "A"),
            _meta("id-2", "Dup", "B"),
        ]
        mock_exec.return_value = {"_success": True, "component_id": "new-id"}
        config = _build_config(
            [_comp(name="Dup", comp_type="process")], conflict_policy="clone"
        )
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        # Verify _execute_component was called with the clone-suffixed name
        call_kwargs = mock_exec.call_args
        resolved_config = call_kwargs.kwargs.get("config") or call_kwargs[1].get("config")
        assert resolved_config["name"].endswith("-clone")
