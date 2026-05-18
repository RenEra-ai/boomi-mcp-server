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


def _db_config(**overrides):
    """A minimal-valid database connector-settings config dict."""
    cfg = {
        "connector_type": "database",
        "driver_id": "sqlserver",
        "auth_mode": "username_password",
        "component_name": "Example SQL Server",
        "folder_name": "Process Library",
        "host": "host.docker.internal",
        "port": 1433,
        "dbname": "ExampleDB",
        "username": "example_user",
        "credential_ref": "credential://example/sqlserver/password",
        "additional": ";encrypt=true;trustServerCertificate=true",
    }
    cfg.update(overrides)
    return cfg


def _db_comp(key="db_connection", name="Example SQL Server",
             action="create", depends_on=None, **config_overrides):
    return IntegrationComponentSpec(
        key=key,
        type="connector-settings",
        action=action,
        name=name,
        config=_db_config(**config_overrides),
        depends_on=depends_on or [],
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


# ---------------------------------------------------------------------------
# M2.2 — database connector-settings preflight in _build_plan
# ---------------------------------------------------------------------------
class TestBuildPlanDatabaseConnectorPreflight:

    @patch(_PATCH_TARGET)
    def test_valid_db_connector_settings_plans_successfully(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config([_db_comp()])
        plan = _build_plan(MagicMock(), config)
        assert plan["_success"] is True
        assert plan["execution_order"] == ["db_connection"]
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step["route"] == "connector_builder_or_xml"
        assert step.get("validation_error") in (None, {})
        assert "validation_error" not in step or step["validation_error"] is None

    @patch(_PATCH_TARGET)
    def test_plan_preserves_name_folder_key_action_depends_on(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(
            key="db_main",
            name="Main SQL Conn",
            depends_on=[],
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["key"] == "db_main"
        assert step["name"] == "Main SQL Conn"
        assert step["type"] == "connector-settings"
        assert step["declared_action"] == "create"
        assert step["depends_on"] == []
        # folder_name lives in config, not on the step — verify it survived
        # through to integration_spec.
        spec_dump = plan["integration_spec"]
        assert spec_dump["components"][0]["config"]["folder_name"] == "Process Library"

    @patch(_PATCH_TARGET)
    def test_missing_credential_ref_marks_plan_as_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp()
        comp.config.pop("credential_ref")
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MISSING_CREDENTIAL_REF"
        assert step["validation_error"]["field"] == "credential_ref"
        assert step["validation_error"]["hint"]

    @patch(_PATCH_TARGET)
    def test_unsupported_driver_marks_plan_as_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config([_db_comp(driver_id="postgres")])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_DRIVER"
        assert step["validation_error"]["field"] == "driver_id"

    @patch(_PATCH_TARGET)
    def test_unsupported_auth_mode_marks_plan_as_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config([_db_comp(auth_mode="windows_integrated")])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_AUTH_MODE"
        assert step["validation_error"]["field"] == "auth_mode"

    @patch(_PATCH_TARGET)
    def test_plaintext_secret_field_marks_plan_as_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config([_db_comp(password="hunter2")])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "password"
        # secret value must not leak into the plan output
        plan_repr = repr(plan)
        assert "hunter2" not in plan_repr

    @patch(_PATCH_TARGET)
    def test_invalid_db_component_does_not_short_circuit_other_components(self, mock_pag):
        mock_pag.return_value = []
        bad_db = _db_comp(key="bad_db", name="Bad DB", driver_id="postgres")
        good_proc = _comp(key="good_proc", name="GoodProc")
        config = _build_config([bad_db, good_proc])
        plan = _build_plan(MagicMock(), config)
        assert set(plan["execution_order"]) == {"bad_db", "good_proc"}
        steps_by_key = {s["key"]: s for s in plan["steps"]}
        assert steps_by_key["bad_db"]["planned_action"] == "error_database_validation"
        assert steps_by_key["good_proc"]["planned_action"] == "create"
        assert "validation_error" not in steps_by_key["good_proc"]

    @patch(_PATCH_TARGET)
    def test_http_connector_settings_skips_database_preflight(self, mock_pag):
        mock_pag.return_value = []
        http_comp = IntegrationComponentSpec(
            key="http_conn",
            type="connector-settings",
            action="create",
            name="HTTP Conn",
            config={"connector_type": "http", "component_name": "HTTP Conn",
                    "url": "https://api.example.com"},
        )
        config = _build_config([http_comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step["route"] == "connector_builder_or_xml"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_reuse_path_skips_database_preflight(self, mock_pag):
        """Reuse never invokes the builder — incomplete config must not block reuse."""
        mock_pag.return_value = [_meta("existing-db-id", "Example SQL Server",
                                       "Process Library", comp_type="connector-settings")]
        comp = _db_comp()
        comp.config.pop("credential_ref")  # would fail builder validation
        config = _build_config([comp], conflict_policy="reuse")
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "reuse"
        assert step["existing_component_id"] == "existing-db-id"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_update_path_skips_database_preflight(self, mock_pag):
        """Update goes through update_connector, not the builder."""
        comp = _db_comp(action="update")
        comp.component_id = "explicit-id"  # bypass ambiguity lookup
        comp.config.pop("credential_ref")
        comp.config.pop("driver_id")
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert "validation_error" not in step
        mock_pag.assert_not_called()

    @patch(_PATCH_TARGET)
    def test_raw_xml_config_skips_database_preflight(self, mock_pag):
        """config.xml is the raw-XML escape hatch — builder is bypassed."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="db_raw",
            type="connector-settings",
            action="create",
            name="Raw DB",
            config={
                "connector_type": "database",
                "xml": "<bns:Component>...pre-built XML...</bns:Component>",
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step["route"] == "connector_builder_or_xml"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_component_name_defaults_from_comp_name_in_preflight(self, mock_pag):
        """_execute_component fills component_name from comp.name — preflight
        must mirror that defaulting so a valid spec doesn't get false-rejected."""
        mock_pag.return_value = []
        comp = _db_comp(name="My Default-Named SQL")
        comp.config.pop("component_name")  # caller relies on comp.name fallback
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_create_clone_path_runs_database_preflight(self, mock_pag):
        """Clone DOES call the builder — preflight must still catch bad config."""
        mock_pag.return_value = [
            _meta("id-1", "Example SQL Server", "A", comp_type="connector-settings"),
            _meta("id-2", "Example SQL Server", "B", comp_type="connector-settings"),
        ]
        comp = _db_comp(driver_id="postgres")
        config = _build_config([comp], conflict_policy="clone")
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_DRIVER"

    # ---- Forbidden-secret scan runs on EVERY db connector-settings step,
    # ---- regardless of apply path (Codex round-2 P2 fix).

    @patch(_PATCH_TARGET)
    def test_update_path_still_rejects_plaintext_password(self, mock_pag):
        """Update bypasses the builder, but plaintext secrets must still be
        rejected and scrubbed so the plan response doesn't echo them."""
        comp = IntegrationComponentSpec(
            key="db_update", type="connector-settings", action="update",
            name="Existing DB", component_id="existing-db-id",
            config={"connector_type": "database",
                    "password": "LEAK_UPDATE_DEADBEEF"},
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "password"
        assert "LEAK_UPDATE_DEADBEEF" not in repr(plan)
        # The scrub also reaches the integration_spec echo.
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["password"] == "[REDACTED]"
        mock_pag.assert_not_called()

    @patch(_PATCH_TARGET)
    def test_reuse_path_still_rejects_plaintext_password(self, mock_pag):
        mock_pag.return_value = [_meta("existing-db-id", "Example SQL Server",
                                       "Process Library", comp_type="connector-settings")]
        comp = _db_comp()
        comp.config["client_secret"] = "LEAK_REUSE_DEADBEEF"
        config = _build_config([comp], conflict_policy="reuse")
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "client_secret"
        assert "LEAK_REUSE_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_raw_xml_path_still_rejects_plaintext_password(self, mock_pag):
        """Even if the apply path is raw-XML (builder bypassed), plaintext
        secrets in config must not leak into the plan response."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="db_raw_leak", type="connector-settings", action="create",
            name="Raw DB",
            config={"connector_type": "database",
                    "xml": "<bns:Component>...</bns:Component>",
                    "token": "LEAK_RAWXML_DEADBEEF"},
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "token"
        assert "LEAK_RAWXML_DEADBEEF" not in repr(plan)


# ---------------------------------------------------------------------------
# M2.2 — _apply_plan fail-fast on database validation
# ---------------------------------------------------------------------------
class TestApplyPlanDatabaseValidationFailFast:

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_database_validation_error(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        config = _build_config([_db_comp(driver_id="postgres")])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert "unresolvable_steps" in result
        assert len(result["unresolvable_steps"]) == 1
        bad = result["unresolvable_steps"][0]
        assert bad["planned_action"] == "error_database_validation"
        assert bad["validation_error"]["error_code"] == "UNSUPPORTED_DB_DRIVER"
        # No mutation occurred — _execute_component must not have been called.
        mock_exec.assert_not_called()

    @patch(_PATCH_TARGET)
    def test_dry_run_surfaces_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp()
        comp.config.pop("credential_ref")
        config = _build_config([comp])
        config["dry_run"] = True
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["dry_run"] is True
        step = result["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MISSING_CREDENTIAL_REF"
