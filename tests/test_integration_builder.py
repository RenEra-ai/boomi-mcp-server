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
    # Process components now require a typed process_kind. Use a minimal valid
    # wrapper_subprocess (one out-of-spec literal process_id call) so generic
    # conflict-policy / ambiguity tests author a structurally-valid process.
    process_config = {
        "name": name,
        "process_kind": "wrapper_subprocess",
        "process_calls": [{"process_id": "99999999-9999-9999-9999-999999999999"}],
    }
    return IntegrationComponentSpec(
        key=key, type=comp_type, action=action, name=name,
        component_id=component_id,
        config=process_config if comp_type == "process" else {},
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


def _db_read_profile_config(**overrides):
    """A minimal-valid database read-profile config dict (Issue #23)."""
    cfg = {
        "component_type": "profile.db",
        "profile_type": "database.read",
        "component_name": "Example Read Profile",
        "folder_name": "Process Library",
        "query": "select 1 as one",
        "output_fields": [{"name": "one"}],
    }
    cfg.update(overrides)
    return cfg


def _db_read_profile_comp(key="db_read_profile", name="Example Read Profile",
                          action="create", depends_on=None, **config_overrides):
    return IntegrationComponentSpec(
        key=key,
        type="profile.db",
        action=action,
        name=name,
        config=_db_read_profile_config(**config_overrides),
        depends_on=depends_on or [],
    )


def _rest_conn_config(**overrides):
    """A minimal-valid REST Client OAuth2 connector-settings config (Issue #24)."""
    cfg = {
        "component_type": "connector-settings",
        "connector_type": "rest",
        "component_name": "Target REST OAuth2 Connection",
        "base_url": "https://api.example.com",
        "auth": "OAUTH2",
        "oauth2": {
            "grant_type": "client_credentials",
            "client_id": "client-id-from-user-or-discovery",
            "client_secret_ref": "credential://target-api/oauth-client-secret",
            "access_token_url": "https://api.example.com/oauth/token",
            "scope": "",
            "credentials_assertion_type": "client_secret",
        },
    }
    cfg.update(overrides)
    return cfg


def _rest_conn_comp(key="target_rest_connection", name="Target REST OAuth2 Connection",
                    action="create", depends_on=None, **config_overrides):
    return IntegrationComponentSpec(
        key=key,
        type="connector-settings",
        action=action,
        name=name,
        config=_rest_conn_config(**config_overrides),
        depends_on=depends_on or [],
    )


def _rest_op_config(**overrides):
    """A minimal-valid REST Client GET/PATCH operation config (Issue #24)."""
    cfg = {
        "component_type": "connector-action",
        "connector_type": "rest",
        "operation_mode": "execute",
        "component_name": "Send Target Record",
        "folder_name": "Process Library",
        "connection_ref_key": "target_rest_connection",
        "method": "PATCH",
        "path": "/v1/items/{id}",
        "request_profile_type": "json",
        "request_profile_id": "$ref:target_json_profile",
        "response_profile_type": "json",
        "payload_source_ref_key": "payload_map",
        "credential_ref": "credential://target-api/headers",
    }
    cfg.update(overrides)
    return cfg


def _rest_op_comp(key="target_rest_operation", name="Send Target Record",
                  action="create",
                  depends_on=("target_rest_connection",
                              "target_json_profile",
                              "payload_map"),
                  **config_overrides):
    return IntegrationComponentSpec(
        key=key,
        type="connector-action",
        action=action,
        name=name,
        config=_rest_op_config(**config_overrides),
        depends_on=list(depends_on),
    )


def _db_get_op_config(**overrides):
    """A minimal-valid database Get-operation config dict (Issue #23)."""
    cfg = {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "get",
        "component_name": "Example DB Query",
        "folder_name": "Process Library",
        "connection_ref_key": "db_connection",
        "read_profile_id": "$ref:db_read_profile",
        "batch_count": 0,
        "max_rows": 0,
    }
    cfg.update(overrides)
    return cfg


def _db_get_op_comp(key="db_query_operation", name="Example DB Query",
                    action="create",
                    depends_on=("db_connection", "db_read_profile"),
                    **config_overrides):
    return IntegrationComponentSpec(
        key=key,
        type="connector-action",
        action=action,
        name=name,
        config=_db_get_op_config(**config_overrides),
        depends_on=list(depends_on),
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
    def test_update_path_runs_database_preflight(self, mock_pag):
        """Codex r2 P2: structured connector updates now invoke the builder
        at apply time (read-merge-write), so plan-time validation must run
        for update too — otherwise an invalid update plans clean and only
        fails after earlier steps have mutated."""
        comp = _db_comp(action="update")
        comp.component_id = "explicit-id"  # bypass ambiguity lookup
        comp.config.pop("credential_ref")
        comp.config.pop("driver_id")
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert "validation_error" in step
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_DRIVER"

    @patch(_PATCH_TARGET)
    def test_metadata_only_update_skips_database_preflight(self, mock_pag):
        """Codex r2 P2: but metadata-only updates (only name/description/folder)
        still route through update_connector's smart-merge, bypassing the
        builder — so they don't trigger plan-time validation."""
        comp = IntegrationComponentSpec(
            key="db_conn",
            type="connector-settings",
            action="update",
            name="Example SQL Server",
            component_id="explicit-id",
            config={
                "connector_type": "database",
                "name": "Renamed Connector",
                "description": "updated description",
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert "validation_error" not in step
        assert step["update_mode"] == "metadata_smart_merge"
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

    # ---- Raw-XML subType inference + multi-secret redaction (Codex round-3 P2s)

    @patch(_PATCH_TARGET)
    def test_raw_xml_with_database_subtype_and_no_connector_type_rejects_secret(self, mock_pag):
        """create_connector's raw-XML path doesn't require connector_type.
        A database connector identified only by subType="database" in the XML
        must still be scanned for plaintext secrets."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="db_inferred", type="connector-settings", action="create",
            name="Raw Inferred DB",
            config={
                "xml": '<bns:Component type="connector-settings" subType="database"/>',
                "password": "LEAK_INFERRED_DEADBEEF",
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "password"
        assert "LEAK_INFERRED_DEADBEEF" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["password"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_raw_xml_with_whitespace_around_subtype_equals_still_rejects(self, mock_pag):
        """XML attribute syntax allows whitespace around the `=`. The
        substring check `'subType="database"' in xml` misses that form;
        the regex tolerates it."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="db_ws", type="connector-settings", action="create",
            name="Raw Whitespace DB",
            config={
                "xml": '<bns:Component subType = "database"/>',
                "password": "LEAK_WHITESPACE_DEADBEEF",
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert "LEAK_WHITESPACE_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_raw_xml_with_database_subtype_single_quote_form_also_rejects(self, mock_pag):
        """Match both attribute-quote variants."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="db_inferred2", type="connector-settings", action="create",
            name="Raw Inferred DB",
            config={
                "xml": "<bns:Component subType='database'/>",
                "secret": "LEAK_SINGLEQUOTE_DEADBEEF",
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert "LEAK_SINGLEQUOTE_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_raw_xml_with_http_subtype_does_not_trigger_db_preflight(self, mock_pag):
        """A non-database raw-XML connector should not be touched by the
        database secret scan (different boundary, builder doesn't apply)."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="http_raw", type="connector-settings", action="create",
            name="Raw HTTP",
            config={
                "xml": '<bns:Component subType="http"/>',
                "password": "LEAK_HTTP_RAWXML_DEADBEEF",
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        # No DB preflight, plan proceeds normally. The secret IS still echoed
        # because HTTP raw-XML is a separate boundary outside this fix's scope.
        assert step["planned_action"] == "create"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_multiple_forbidden_secrets_all_redacted(self, mock_pag):
        """scan_forbidden_secret_fields returns the first offender, but the
        spec echo must scrub every forbidden field — otherwise a config with
        password + token leaks the second one."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="db_multi", type="connector-settings", action="update",
            name="Multi-Secret DB", component_id="existing-id",
            config={
                "connector_type": "database",
                "password": "LEAK_MULTI_A_DEADBEEF",
                "token": "LEAK_MULTI_B_DEADBEEF",
                "access_token": "LEAK_MULTI_C_DEADBEEF",
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        # Error envelope names ONE field (stop-on-first), that's fine.
        # But the spec echo MUST redact all of them.
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["password"] == "[REDACTED]"
        assert echoed["token"] == "[REDACTED]"
        assert echoed["access_token"] == "[REDACTED]"
        plan_repr = repr(plan)
        assert "LEAK_MULTI_A_DEADBEEF" not in plan_repr
        assert "LEAK_MULTI_B_DEADBEEF" not in plan_repr
        assert "LEAK_MULTI_C_DEADBEEF" not in plan_repr

    # -------------------------------------------------------------------
    # Issue #31 — shape discriminator, pooling, write_options
    # -------------------------------------------------------------------

    @patch(_PATCH_TARGET)
    def test_pooling_enabled_plans_successfully(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(pooling={"enabled": True, "max_active": 50})
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        assert plan["_success"]
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step["route"] == "connector_builder_or_xml"
        assert step.get("validation_error") is None

    @patch(_PATCH_TARGET)
    def test_custom_driver_with_wrong_shape_field_marks_plan_as_database_validation_error(self, mock_pag):
        """Issue #31: Custom is buildable now, but uses the custom_url shape.
        A caller mixing host_port_db fields (host/dbname) with driver_id=custom
        must fail with DATABASE_CONNECTOR_VALIDATION_FAILED before mutation."""
        mock_pag.return_value = []
        # _db_comp defaults carry host/port/dbname (host_port_db fields).
        # validate_config flags the missing custom_class_name first — that's
        # the contract: required-field checks run before forbidden-field
        # checks, so the structured error nails the missing required field.
        comp = _db_comp(driver_id="custom")
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "DATABASE_CONNECTOR_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "custom_class_name"

    @patch(_PATCH_TARGET)
    def test_custom_driver_with_host_field_marks_plan_as_database_validation_error(self, mock_pag):
        """custom_url shape rejects host (and port/dbname/additional) outright.
        Once the required custom fields are present, the forbidden walker fires."""
        mock_pag.return_value = []
        # Build a valid custom_url config first, then add a forbidden host.
        comp = IntegrationComponentSpec(
            key="db_custom_with_host", type="connector-settings", action="create",
            name="Custom with Host", config={
                "connector_type": "database",
                "driver_id": "custom",
                "auth_mode": "username_password",
                "component_name": "Custom with Host",
                "username": "u",
                "credential_ref": "credential://x/y",
                "custom_class_name": "com.example.Driver",
                "connection_url": "jdbc:example://host/db",
                "host": "host.example.com",  # ← forbidden on custom_url
            },
        )
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "DATABASE_CONNECTOR_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "host"
        assert "custom_url" in step["validation_error"]["hint"]

    @patch(_PATCH_TARGET)
    def test_valid_custom_driver_plans_without_validation_error(self, mock_pag):
        """A custom_url config with custom_class_name + connection_url and no
        host_port_db fields passes preflight cleanly."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="db_custom_ok", type="connector-settings", action="create",
            name="Custom OK", config={
                "connector_type": "database",
                "driver_id": "custom",
                "auth_mode": "username_password",
                "component_name": "Custom OK",
                "username": "u",
                "credential_ref": "credential://x/y",
                "custom_class_name": "com.example.Driver",
                "connection_url": "jdbc:example://host/db",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step["route"] == "connector_builder_or_xml"
        assert step.get("validation_error") is None

    @patch(_PATCH_TARGET)
    def test_sap_hana_missing_port_marks_plan_as_database_validation_error(self, mock_pag):
        """SAP HANA has no verified default port — caller must supply it."""
        mock_pag.return_value = []
        comp = _db_comp(driver_id="sap_hana")
        # Remove port from the default _db_config so we trigger the check.
        del comp.config["port"]
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "DATABASE_CONNECTOR_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "port"

    @patch(_PATCH_TARGET)
    def test_invalid_pooling_marks_plan_as_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(pooling={"bogus_key": 1})
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "DATABASE_POOLING_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "pooling.bogus_key"

    @patch(_PATCH_TARGET)
    def test_invalid_write_options_marks_plan_as_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(write_options={"write_sql_to_file": True})  # missing sql_file_path
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "write_options.sql_file_path"

    @patch(_PATCH_TARGET)
    def test_update_path_runs_full_validation_and_secret_scan(self, mock_pag):
        """Codex r2 P2: structured updates invoke the builder at apply
        time, so plan-time validation must catch builder errors AND
        secret-scan leaks. Metadata-only updates skip the builder, but
        bogus body fields (pooling, host) do not — they trigger the
        full validator at plan time."""
        mock_pag.return_value = []
        # Bogus pooling on update path: now DOES trip shape/pooling validation
        # because the structured update apply will invoke the builder.
        comp_update = IntegrationComponentSpec(
            key="db_update", type="connector-settings", action="update",
            name="Existing DB", component_id="existing-db-id",
            config={"connector_type": "database",
                    "pooling": {"totally_bogus_key": True}},
        )
        plan_update = _build_plan(MagicMock(), _build_config([comp_update]))
        step_update = plan_update["steps"][0]
        assert step_update["planned_action"] == "error_database_validation"

        # Plaintext secret on update path remains caught by the secret scan
        # (which runs regardless of will_invoke_builder gating).
        comp_secret = IntegrationComponentSpec(
            key="db_secret", type="connector-settings", action="update",
            name="Existing DB", component_id="existing-db-id",
            config={"connector_type": "database", "password": "LEAK_UPDATE_DEADBEEF"},
        )
        plan_secret = _build_plan(MagicMock(), _build_config([comp_secret]))
        step_secret = plan_secret["steps"][0]
        assert step_secret["planned_action"] == "error_database_validation"
        assert step_secret["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert "LEAK_UPDATE_DEADBEEF" not in repr(plan_secret)

    @patch(_PATCH_TARGET)
    def test_nested_secret_in_pooling_block_redacted_in_plan_output(self, mock_pag):
        """Codex P1 (post-Issue #31): a forbidden secret-shaped key inside the
        new pooling/write_options dicts must (a) trip the plaintext-secret
        error (NOT the sub-block validator), and (b) be scrubbed from the
        plan's spec echo at any depth."""
        mock_pag.return_value = []
        comp = _db_comp(pooling={"password": "LEAK_NESTED_POOLING_DEADBEEF"})
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "pooling.password"
        # Spec echo must not contain the leaked value
        echoed_pooling = plan["integration_spec"]["components"][0]["config"]["pooling"]
        assert echoed_pooling["password"] == "[REDACTED]"
        assert "LEAK_NESTED_POOLING_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_nested_secret_in_write_options_block_redacted_in_plan_output(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(write_options={"secret": "LEAK_NESTED_WO_DEADBEEF"})
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "write_options.secret"
        echoed_wo = plan["integration_spec"]["components"][0]["config"]["write_options"]
        assert echoed_wo["secret"] == "[REDACTED]"
        assert "LEAK_NESTED_WO_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_secret_inside_list_of_dicts_redacted_in_plan_output(self, mock_pag):
        """Codex P2 (post-P1): the builder ignores unknown top-level keys,
        so a caller can smuggle a list-of-dicts (e.g. `extra: [{password:...}]`)
        past validate_config. Without descent into list elements, scan misses
        it and the plan echoes the plaintext value. Must be PLAINTEXT_SECRET_REJECTED
        with a path like `extra[0].password`, value redacted in echo, and
        absent from the response payload."""
        mock_pag.return_value = []
        comp = _db_comp(extra=[{"password": "LEAK_LIST_PLAN_DEADBEEF"}])
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "extra[0].password"
        echoed = plan["integration_spec"]["components"][0]["config"]["extra"]
        assert echoed == [{"password": "[REDACTED]"}]
        assert "LEAK_LIST_PLAN_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_secret_in_list_of_lists_redacted_in_plan_output(self, mock_pag):
        """Codex P2 follow-up #2: lists nested inside lists must also be
        walked. Before the generalized walker, `matrix=[[{password:...}]]`
        slipped past — planned_action='create', plan echoed the leak."""
        mock_pag.return_value = []
        comp = _db_comp(matrix=[[{"password": "LEAK_LIST_OF_LIST_PLAN_DEADBEEF"}]])
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "matrix[0][0].password"
        echoed = plan["integration_spec"]["components"][0]["config"]["matrix"]
        assert echoed == [[{"password": "[REDACTED]"}]]
        assert "LEAK_LIST_OF_LIST_PLAN_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_secret_at_list_index_two_redacted_in_plan_output(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(
            extra=[{"safe": "ok"}, {"other": "ok"}, {"token": "LEAK_IDX2_PLAN_DEADBEEF"}],
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["field"] == "extra[2].token"
        echoed = plan["integration_spec"]["components"][0]["config"]["extra"]
        assert echoed[2]["token"] == "[REDACTED]"
        assert echoed[0] == {"safe": "ok"}  # non-secret entries preserved
        assert echoed[1] == {"other": "ok"}
        assert "LEAK_IDX2_PLAN_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_mixed_top_level_and_nested_secrets_all_redacted_in_plan_output(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(
            password="LEAK_TOP_DEADBEEF",  # top-level wins for error message
            pooling={"token": "LEAK_NESTED_POOL_DEADBEEF"},
            write_options={"access_token": "LEAK_NESTED_WO_DEADBEEF"},
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        # Top-level offender wins the error
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "password"
        # But the echo redacts ALL three
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["password"] == "[REDACTED]"
        assert echoed["pooling"]["token"] == "[REDACTED]"
        assert echoed["write_options"]["access_token"] == "[REDACTED]"
        plan_repr = repr(plan)
        assert "LEAK_TOP_DEADBEEF" not in plan_repr
        assert "LEAK_NESTED_POOL_DEADBEEF" not in plan_repr
        assert "LEAK_NESTED_WO_DEADBEEF" not in plan_repr

    @patch(_PATCH_TARGET)
    def test_raw_xml_path_skips_shape_validation_but_keeps_secret_scan(self, mock_pag):
        """Raw-XML create path bypasses full builder validation — custom driver
        XML CAN be supplied via raw XML (documented escape hatch). Plaintext
        secrets are still rejected by the secret scan."""
        mock_pag.return_value = []
        # Raw XML for what would be a custom driver — must NOT trip shape check
        comp_custom_xml = IntegrationComponentSpec(
            key="db_custom_xml", type="connector-settings", action="create",
            name="Custom Via Raw XML",
            config={
                "xml": ('<bns:Component subType="database">'
                        '<DatabaseConnectionSettings driverId="custom"/></bns:Component>'),
            },
        )
        plan_xml = _build_plan(MagicMock(), _build_config([comp_custom_xml]))
        step_xml = plan_xml["steps"][0]
        # Plan resolves to create (no shape preflight on raw-XML path)
        assert step_xml["planned_action"] == "create"
        assert step_xml.get("validation_error") is None

        # But raw XML with plaintext secret is still caught
        comp_secret_xml = IntegrationComponentSpec(
            key="db_leak_xml", type="connector-settings", action="create",
            name="Leaky DB",
            config={
                "xml": '<bns:Component subType="database"/>',
                "password": "LEAK_RAWXML_DEADBEEF",
            },
        )
        plan_secret = _build_plan(MagicMock(), _build_config([comp_secret_xml]))
        step_secret = plan_secret["steps"][0]
        assert step_secret["planned_action"] == "error_database_validation"
        assert step_secret["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"


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

    # Issue #31

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_database_shape_mismatch(self, mock_pag, mock_exec):
        """Issue #31: Custom is buildable but uses the custom_url shape. The
        _db_comp default config carries host_port_db fields, so dispatching
        as driver_id=custom fails fast with the structured validation error
        (now field=custom_class_name) before any component execution."""
        mock_pag.return_value = []
        config = _build_config([_db_comp(driver_id="custom")])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert len(result["unresolvable_steps"]) == 1
        bad = result["unresolvable_steps"][0]
        assert bad["planned_action"] == "error_database_validation"
        assert bad["validation_error"]["error_code"] == "DATABASE_CONNECTOR_VALIDATION_FAILED"
        mock_exec.assert_not_called()

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_pooling_validation_error(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        config = _build_config([_db_comp(pooling={"bogus": 1})])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert len(result["unresolvable_steps"]) == 1
        bad = result["unresolvable_steps"][0]
        assert bad["validation_error"]["error_code"] == "DATABASE_POOLING_VALIDATION_FAILED"
        mock_exec.assert_not_called()

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_write_options_validation_error(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        config = _build_config(
            [_db_comp(write_options={"write_sql_to_file": True})],  # missing sql_file_path
        )
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert len(result["unresolvable_steps"]) == 1
        bad = result["unresolvable_steps"][0]
        assert bad["validation_error"]["error_code"] == "DATABASE_WRITE_OPTIONS_VALIDATION_FAILED"
        mock_exec.assert_not_called()


# ===========================================================================
# Issue #23 — Database Read Profile + Get Operation preflight & apply
# ===========================================================================


class TestBuildPlanDatabaseReadProfilePreflight:
    """Issue #23 — preflight contract for profile.db + database.read components."""

    @patch(_PATCH_TARGET)
    def test_valid_read_profile_plans_without_validation_error(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config([_db_read_profile_comp()])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step.get("validation_error") is None
        assert step["route"] == "profile_builder_or_xml"

    @patch(_PATCH_TARGET)
    def test_missing_query_surfaces_missing_db_query(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config["query"] = ""
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MISSING_DB_QUERY"

    @patch(_PATCH_TARGET)
    def test_missing_output_fields_surfaces_missing_db_output_fields(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config["output_fields"] = []
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MISSING_DB_OUTPUT_FIELDS"

    @patch(_PATCH_TARGET)
    def test_unsupported_profile_type_surfaces_unsupported_db_profile_mode(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config["profile_type"] = "database.write"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_PROFILE_MODE"

    @patch(_PATCH_TARGET)
    def test_plaintext_secret_in_read_profile_is_scrubbed_in_plan_output(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config["password"] = "leaked"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        # Spec echo must redact the plaintext value.
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["password"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_raw_xml_payload_skips_builder_validation(self, mock_pag):
        # When the caller supplies raw XML, preflight must not run validate_config
        # (the XML bypasses the builder). The scan still runs to catch leaked
        # secrets in the spec dump, but field-level validation does not.
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config["xml"] = "<bns:Component type='profile.db'/>"
        # Strip required structured fields to confirm builder validation is skipped.
        comp.config.pop("query")
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step.get("validation_error") is None


class TestBuildPlanDatabaseGetOperationPreflight:
    """Issue #23 — preflight contract for connector-action + database.get."""

    @patch(_PATCH_TARGET)
    def test_valid_get_op_with_full_deps_plans_without_error(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            _db_get_op_comp(),
        ])
        plan = _build_plan(MagicMock(), config)
        # Execution order must place connection -> profile -> operation.
        assert plan["execution_order"] == [
            "db_connection",
            "db_read_profile",
            "db_query_operation",
        ]
        # Every step plans cleanly.
        for step in plan["steps"]:
            assert step.get("validation_error") is None
        # Route assignments.
        routes = {s["key"]: s["route"] for s in plan["steps"]}
        assert routes["db_connection"] == "connector_builder_or_xml"
        assert routes["db_read_profile"] == "profile_builder_or_xml"
        assert routes["db_query_operation"] == "connector_builder_or_xml"

    @patch(_PATCH_TARGET)
    def test_operation_mode_send_is_rejected_with_issue32_hint(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_get_op_comp(operation_mode="send")
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            comp,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["planned_action"] == "error_database_validation"
        assert op_step["validation_error"]["error_code"] == "UNSUPPORTED_DB_OPERATION_MODE"
        assert "#32" in (op_step["validation_error"]["hint"] or "")

    @patch(_PATCH_TARGET)
    def test_missing_read_profile_id_surfaces_missing_db_read_profile_ref(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_get_op_comp()
        comp.config.pop("read_profile_id")
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            comp,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["planned_action"] == "error_database_validation"
        assert op_step["validation_error"]["error_code"] == "MISSING_DB_READ_PROFILE_REF"

    @patch(_PATCH_TARGET)
    def test_missing_connection_ref_key_surfaces_missing_db_dependency(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_get_op_comp()
        comp.config.pop("connection_ref_key")
        comp.depends_on = ["db_read_profile"]
        plan = _build_plan(MagicMock(), _build_config([
            _db_read_profile_comp(),
            comp,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["planned_action"] == "error_database_validation"
        assert op_step["validation_error"]["error_code"] == "MISSING_DB_DEPENDENCY"
        assert op_step["validation_error"]["field"] == "connection_ref_key"

    @patch(_PATCH_TARGET)
    def test_connection_ref_key_not_in_depends_on_surfaces_missing_db_dependency(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_get_op_comp()
        comp.depends_on = ["db_read_profile"]  # forgot the connection
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            comp,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["planned_action"] == "error_database_validation"
        assert op_step["validation_error"]["error_code"] == "MISSING_DB_DEPENDENCY"

    @patch(_PATCH_TARGET)
    def test_ref_target_not_in_depends_on_surfaces_missing_db_dependency(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_get_op_comp()
        comp.depends_on = ["db_connection"]  # forgot the profile
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            comp,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["planned_action"] == "error_database_validation"
        assert op_step["validation_error"]["error_code"] == "MISSING_DB_DEPENDENCY"

    @patch(_PATCH_TARGET)
    def test_uuid_read_profile_id_does_not_require_depends_on(self, mock_pag):
        # When the caller passes a literal UUID instead of a $ref token, the
        # profile is assumed to exist already — no $ref resolution and no
        # depends_on cross-check is required for that field. (connection_ref_key
        # is still mandatory.)
        mock_pag.return_value = []
        comp = _db_get_op_comp(read_profile_id="abc-123-def")
        comp.depends_on = ["db_connection"]
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            comp,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step.get("validation_error") is None

    @patch(_PATCH_TARGET)
    def test_link_element_is_rejected_at_plan_time(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_get_op_comp(link_element="some_field")
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            comp,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["planned_action"] == "error_database_validation"
        assert op_step["validation_error"]["error_code"] == "UNSUPPORTED_DB_GET_FIELD"


class TestApplyPlanDatabaseProfileAndGetFailFast:
    """Issue #23 — apply must fail-fast on read-profile or Get-op errors."""

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_read_profile_validation_error(
        self, mock_pag, mock_exec
    ):
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config["query"] = ""  # MISSING_DB_QUERY
        config = _build_config([comp])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        bad = result["unresolvable_steps"][0]
        assert bad["validation_error"]["error_code"] == "MISSING_DB_QUERY"
        mock_exec.assert_not_called()

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_get_op_send_mode(
        self, mock_pag, mock_exec
    ):
        mock_pag.return_value = []
        comp = _db_get_op_comp(operation_mode="send")
        config = _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            comp,
        ])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        codes = {s["validation_error"]["error_code"] for s in result["unresolvable_steps"]}
        assert "UNSUPPORTED_DB_OPERATION_MODE" in codes
        mock_exec.assert_not_called()

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_resolves_ref_token_to_created_profile_id(
        self, mock_pag, mock_exec
    ):
        # End-to-end-style: db_connection -> db_read_profile -> db_query_operation
        # When the operation step executes, the $ref:db_read_profile in its
        # config must have been substituted with the profile's component_id.
        mock_pag.return_value = []
        mock_exec.side_effect = [
            {"_success": True, "component_id": "conn-001", "type": "connector-settings"},
            {"_success": True, "component_id": "profile-002", "type": "profile.db"},
            {"_success": True, "component_id": "op-003", "type": "connector-action"},
        ]
        config = _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            _db_get_op_comp(),
        ])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        # Inspect the third call (db_query_operation) — its resolved config
        # must contain the actual profile component_id, not the $ref token.
        third_call = mock_exec.call_args_list[2]
        resolved_config = third_call.kwargs["config"]
        assert resolved_config["read_profile_id"] == "profile-002"


# ===========================================================================
# Codex review f398b35 follow-up — regression tests for the 4 P2 items
# ===========================================================================


class TestCodexReviewF398b35Followup:
    """Regression tests for the four Codex P2 items against commit f398b35.

    Items 1+2: _execute_component must inject comp.type into payload so the
    apply-time dispatcher predicates (config["component_type"]) align with
    the plan-time predicates (comp.type).

    Items 3+4: preflight gates must run the builder validator for every
    profile.db component (regardless of profile_type) and every database
    connector-action (regardless of operation_mode), so malformed shapes
    can't plan as clean creates with un-redacted secret echoes.
    """

    # --- Items 1+2: component_type injection ---

    @patch("src.boomi_mcp.categories.integration_builder.create_connector")
    @patch("src.boomi_mcp.categories.integration_builder._resolve_existing_components")
    @patch(_PATCH_TARGET)
    def test_apply_injects_component_type_for_connector_action(
        self, mock_pag, mock_resolve, mock_create_connector
    ):
        mock_pag.return_value = []
        mock_resolve.return_value = []
        mock_create_connector.side_effect = [
            {"_success": True, "component_id": "conn-001", "type": "connector-settings"},
            {"_success": True, "component_id": "op-003", "type": "connector-action"},
        ]
        op = _db_get_op_comp(depends_on=["db_connection"])
        op.config.pop("component_type")  # Caller omitted the duplicate key
        # Use a literal read_profile_id so we don't also need a profile step.
        op.config["read_profile_id"] = "literal-profile-id"
        config = _build_config([_db_comp(), op])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        # Second call is the operation — its payload must have component_type set.
        op_call = mock_create_connector.call_args_list[1]
        op_payload = op_call.args[2]  # create_connector(client, profile, config)
        assert op_payload["component_type"] == "connector-action"

    @patch("src.boomi_mcp.categories.integration_builder.create_component")
    @patch("src.boomi_mcp.categories.integration_builder._resolve_existing_components")
    @patch(_PATCH_TARGET)
    def test_apply_injects_component_type_for_profile_db(
        self, mock_pag, mock_resolve, mock_create_component
    ):
        mock_pag.return_value = []
        mock_resolve.return_value = []
        mock_create_component.return_value = {
            "_success": True, "component_id": "profile-002", "type": "profile.db"
        }
        profile = _db_read_profile_comp()
        profile.config.pop("component_type")  # Caller omitted the duplicate key
        config = _build_config([profile])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        call = mock_create_component.call_args_list[0]
        payload = call.args[2]  # create_component(client, profile, config)
        assert payload["component_type"] == "profile.db"

    # --- Items 3+4: widened preflight gates ---

    @patch(_PATCH_TARGET)
    def test_profile_db_without_profile_type_surfaces_unsupported_db_profile_mode(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config.pop("profile_type")
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_PROFILE_MODE"

    @patch(_PATCH_TARGET)
    def test_profile_db_without_profile_type_redacts_plaintext_secrets(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_read_profile_comp()
        comp.config.pop("profile_type")
        comp.config["password"] = "leaked"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        # Spec echo must redact even though profile_type is missing.
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["password"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_connector_action_database_unknown_mode_surfaces_unsupported_db_operation_mode(self, mock_pag):
        mock_pag.return_value = []
        op = _db_get_op_comp(operation_mode="upsert")
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["planned_action"] == "error_database_validation"
        assert op_step["validation_error"]["error_code"] == "UNSUPPORTED_DB_OPERATION_MODE"

    @patch(_PATCH_TARGET)
    def test_connector_action_database_blank_mode_redacts_plaintext_secrets(self, mock_pag):
        mock_pag.return_value = []
        op = _db_get_op_comp(operation_mode="")
        op.config["token"] = "leaked"
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            _db_read_profile_comp(),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        assert op_step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        # Spec echo must redact the token even though operation_mode is blank.
        echoed_op = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == "db_query_operation"
        )
        assert echoed_op["config"]["token"] == "[REDACTED]"

    # --- Follow-up review items (post-d48c30e):
    # P1 — _execute_component must inject comp.name into payload["component_name"]
    #      for profile.db so plan and apply agree on what gets validated.
    # P2 — _apply_clone_suffix must rename profile.db too (it now participates
    #      in metadata lookup, so conflict_policy=clone is reachable).

    @patch("src.boomi_mcp.categories.integration_builder.create_component")
    @patch("src.boomi_mcp.categories.integration_builder._resolve_existing_components")
    @patch(_PATCH_TARGET)
    def test_apply_injects_component_name_for_profile_db_when_config_omits_it(
        self, mock_pag, mock_resolve, mock_create_component
    ):
        # Plan-time validation (line ~565 in _build_plan) seeds
        # effective_config["component_name"] from comp.name before calling
        # validate_config, so a spec like this plans cleanly. _execute_component
        # must mirror that so apply doesn't fail with
        # DATABASE_OPERATION_VALIDATION_FAILED: component_name is required.
        mock_pag.return_value = []
        mock_resolve.return_value = []
        mock_create_component.return_value = {
            "_success": True, "component_id": "profile-002", "type": "profile.db"
        }
        profile = _db_read_profile_comp(name="Example Read Profile")
        profile.config.pop("component_name")  # Caller omitted the duplicate key
        config = _build_config([profile])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        payload = mock_create_component.call_args_list[0].args[2]
        assert payload["component_name"] == "Example Read Profile"

    def test_apply_clone_suffix_renames_profile_db_component(self):
        from src.boomi_mcp.categories.integration_builder import _apply_clone_suffix
        comp = _db_read_profile_comp(name="Example Read Profile")
        cloned = _apply_clone_suffix(comp, dict(comp.config))
        assert cloned["component_name"] == "Example Read Profile-clone"

    def test_apply_clone_suffix_renames_profile_db_using_comp_name_when_config_omits_it(self):
        from src.boomi_mcp.categories.integration_builder import _apply_clone_suffix
        comp = _db_read_profile_comp(name="Example Read Profile")
        cfg = dict(comp.config)
        cfg.pop("component_name", None)
        cloned = _apply_clone_suffix(comp, cfg)
        # Falls back to comp.name when config has no component_name.
        assert cloned["component_name"] == "Example Read Profile-clone"


# ===========================================================================
# M2.3 follow-up — Stored Procedure Read profile preflight & apply
#
# Mirrors TestBuildPlanDatabaseReadProfilePreflight + apply tests but with
# profile_type="database.stored_procedure_read" and procedure_name in place
# of query. The Get-op layer is unchanged — it references the SP profile by
# ID via the same $ref:db_sp_read_profile mechanism.
# ===========================================================================


def _db_sp_read_profile_config(**overrides):
    """Minimal valid Stored Procedure Read profile config."""
    cfg = {
        "component_type": "profile.db",
        "profile_type": "database.stored_procedure_read",
        "component_name": "Example SP Read Profile",
        "folder_name": "Process Library",
        "procedure_name": "schema.proc",
        "output_fields": [{"name": "col_a"}],
    }
    cfg.update(overrides)
    return cfg


def _db_sp_read_profile_comp(key="db_sp_read_profile",
                              name="Example SP Read Profile",
                              action="create", depends_on=None,
                              **config_overrides):
    return IntegrationComponentSpec(
        key=key,
        type="profile.db",
        action=action,
        name=name,
        config=_db_sp_read_profile_config(**config_overrides),
        depends_on=depends_on or [],
    )


def _db_get_op_for_sp_comp(key="db_query_operation", name="Example DB Query",
                            action="create",
                            depends_on=("db_connection", "db_sp_read_profile"),
                            **config_overrides):
    """A Get operation that references the SP profile via $ref."""
    defaults = {
        "component_type": "connector-action",
        "connector_type": "database",
        "operation_mode": "get",
        "component_name": "Example DB Query",
        "folder_name": "Process Library",
        "connection_ref_key": "db_connection",
        "read_profile_id": "$ref:db_sp_read_profile",
        "batch_count": 0,
        "max_rows": 0,
    }
    defaults.update(config_overrides)
    return IntegrationComponentSpec(
        key=key,
        type="connector-action",
        action=action,
        name=name,
        config=defaults,
        depends_on=list(depends_on),
    )


class TestBuildPlanDatabaseStoredProcedureReadProfilePreflight:
    """Preflight contract for profile.db + database.stored_procedure_read."""

    @patch(_PATCH_TARGET)
    def test_valid_sp_profile_plans_without_validation_error(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([_db_sp_read_profile_comp()]))
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step.get("validation_error") is None
        assert step["route"] == "profile_builder_or_xml"

    @patch(_PATCH_TARGET)
    def test_missing_procedure_name_surfaces_structured_error(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        comp.config["procedure_name"] = ""
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MISSING_DB_PROCEDURE_NAME"

    @patch(_PATCH_TARGET)
    def test_missing_output_fields_surfaces_missing_db_output_fields(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        comp.config["output_fields"] = []
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MISSING_DB_OUTPUT_FIELDS"

    @patch(_PATCH_TARGET)
    def test_invalid_parameter_mode_surfaces_structured_error(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        comp.config["parameters"] = [{"name": "p", "mode": "garbage"}]
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "INVALID_DB_PARAMETER_MODE"

    @patch(_PATCH_TARGET)
    def test_multiple_return_parameters_surface_structured_error(self, mock_pag):
        # Boomi reference doc: only one return parameter allowed per statement.
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        comp.config["parameters"] = [
            {"name": "r1", "mode": "return"},
            {"name": "r2", "mode": "return"},
        ]
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MULTIPLE_DB_RETURN_PARAMETERS"

    @patch(_PATCH_TARGET)
    def test_plaintext_secret_in_parameter_dict_is_scrubbed_in_plan_output(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        comp.config["parameters"] = [{"name": "p", "password": "leak"}]
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["parameters"][0]["password"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_sp_profile_routes_via_profile_builder(self, mock_pag):
        # Sanity: the preflight gate must dispatch to the SP builder via the
        # registry, not silently default to the Select builder.
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        # If we accidentally used DatabaseReadProfileBuilder.validate_config,
        # we'd get UNSUPPORTED_DB_PROFILE_MODE because that builder rejects
        # any profile_type other than database.read.
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step.get("validation_error") is None

    @patch(_PATCH_TARGET)
    def test_unknown_profile_type_lists_both_supported_protocols(self, mock_pag):
        # Bad/unknown profile_type should surface UNSUPPORTED_DB_PROFILE_MODE
        # with a hint that mentions both supported protocols.
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        comp.config["profile_type"] = "database.bogus"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_PROFILE_MODE"
        hint = step["validation_error"].get("hint") or ""
        assert "database.read" in hint
        assert "database.stored_procedure_read" in hint


class TestBuildPlanDatabaseGetOperationWithSpProfile:
    """Get-op referencing an SP profile must plan + execute identically to one
    referencing a Select profile."""

    @patch(_PATCH_TARGET)
    def test_get_op_with_sp_profile_dep_plans_cleanly(self, mock_pag):
        mock_pag.return_value = []
        config = _build_config([
            _db_comp(),
            _db_sp_read_profile_comp(),
            _db_get_op_for_sp_comp(),
        ])
        plan = _build_plan(MagicMock(), config)
        assert plan["execution_order"] == [
            "db_connection",
            "db_sp_read_profile",
            "db_query_operation",
        ]
        for step in plan["steps"]:
            assert step.get("validation_error") is None


class TestApplyPlanStoredProcedureProfileAndGet:
    """Apply path for SP profile + Get-op."""

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_fails_before_execution_on_missing_procedure_name(
        self, mock_pag, mock_exec
    ):
        mock_pag.return_value = []
        comp = _db_sp_read_profile_comp()
        comp.config["procedure_name"] = ""
        config = _build_config([comp])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        bad = result["unresolvable_steps"][0]
        assert bad["validation_error"]["error_code"] == "MISSING_DB_PROCEDURE_NAME"
        mock_exec.assert_not_called()

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_resolves_ref_token_from_sp_profile_to_get_op(
        self, mock_pag, mock_exec
    ):
        mock_pag.return_value = []
        mock_exec.side_effect = [
            {"_success": True, "component_id": "conn-001", "type": "connector-settings"},
            {"_success": True, "component_id": "sp-profile-002", "type": "profile.db"},
            {"_success": True, "component_id": "op-003", "type": "connector-action"},
        ]
        config = _build_config([
            _db_comp(),
            _db_sp_read_profile_comp(),
            _db_get_op_for_sp_comp(),
        ])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        # The Get operation's resolved config must reference the SP profile's
        # component_id (substituted from $ref:db_sp_read_profile).
        third_call = mock_exec.call_args_list[2]
        resolved_config = third_call.kwargs["config"]
        assert resolved_config["read_profile_id"] == "sp-profile-002"

    def test_apply_clone_suffix_renames_sp_profile_component(self):
        from src.boomi_mcp.categories.integration_builder import _apply_clone_suffix
        comp = _db_sp_read_profile_comp(name="Example SP Read Profile")
        cloned = _apply_clone_suffix(comp, dict(comp.config))
        assert cloned["component_name"] == "Example SP Read Profile-clone"

    @patch("src.boomi_mcp.categories.integration_builder.create_component")
    @patch("src.boomi_mcp.categories.integration_builder._resolve_existing_components")
    @patch(_PATCH_TARGET)
    def test_apply_injects_component_type_for_sp_profile_db(
        self, mock_pag, mock_resolve, mock_create_component
    ):
        mock_pag.return_value = []
        mock_resolve.return_value = []
        mock_create_component.return_value = {
            "_success": True, "component_id": "sp-profile-002", "type": "profile.db"
        }
        profile = _db_sp_read_profile_comp()
        profile.config.pop("component_type")
        config = _build_config([profile])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        payload = mock_create_component.call_args_list[0].args[2]
        assert payload["component_type"] == "profile.db"
        assert payload["component_name"] == "Example SP Read Profile"


# ---------------------------------------------------------------------------
# Issue #24 — REST Client connector-settings + connector-action preflight
# ---------------------------------------------------------------------------


def _rest_supporting_components():
    """Stub `target_json_profile` and `payload_map` placeholders so they
    satisfy depends_on without dragging in real profile/map builders."""
    profile = IntegrationComponentSpec(
        key="target_json_profile",
        type="component",
        action="create",
        name="Target JSON Profile",
        config={"name": "Target JSON Profile", "type": "profile.json"},
    )
    payload_map = IntegrationComponentSpec(
        key="payload_map",
        type="component",
        action="create",
        name="Payload Map",
        config={"name": "Payload Map", "type": "transform.map"},
    )
    return [profile, payload_map]


class TestBuildPlanRestPreflight:
    """Issue #24 — REST Client connector-settings + REST operation preflight."""

    @patch(_PATCH_TARGET)
    def test_valid_rest_connection_plus_operation_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        components = [
            _rest_conn_comp(),
            *_rest_supporting_components(),
            _rest_op_comp(),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        assert plan["_success"] is True
        order = plan["execution_order"]
        assert order.index("target_rest_connection") < order.index("target_rest_operation")
        steps_by_key = {s["key"]: s for s in plan["steps"]}
        for key in ("target_rest_connection", "target_rest_operation"):
            step = steps_by_key[key]
            assert step["planned_action"] == "create"
            assert step["route"] == "connector_builder_or_xml"
            assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_missing_connection_ref_key_marks_step_unresolvable(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(depends_on=("target_json_profile", "payload_map"))
        op.config.pop("connection_ref_key")
        components = [*_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_CONNECTION_REF_REQUIRED"
        assert step["validation_error"]["field"] == "connection_ref_key"

    @patch(_PATCH_TARGET)
    def test_connection_ref_key_not_in_depends_on_marks_unresolvable(self, mock_pag):
        mock_pag.return_value = []
        # connection_ref_key present in config but not in depends_on.
        op = _rest_op_comp(depends_on=("target_json_profile", "payload_map"))
        components = [
            _rest_conn_comp(),
            *_rest_supporting_components(),
            op,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_DEPENDENCY_REQUIRED"
        assert step["validation_error"]["field"] == "depends_on"

    @patch(_PATCH_TARGET)
    def test_request_profile_ref_missing_from_depends_on_marks_unresolvable(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(
            depends_on=("target_rest_connection", "payload_map"),
            request_profile_id="$ref:missing_profile",
        )
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_DEPENDENCY_REQUIRED"
        assert step["validation_error"]["field"] == "depends_on"

    @patch(_PATCH_TARGET)
    def test_response_profile_ref_missing_from_depends_on_marks_unresolvable(self, mock_pag):
        """Regression: response_profile_id $ref must be checked the same way
        request_profile_id is (codex review item #3 from the superseded HTTP
        implementation)."""
        mock_pag.return_value = []
        op = _rest_op_comp(
            depends_on=("target_rest_connection", "target_json_profile", "payload_map"),
            response_profile_id="$ref:missing_response_profile",
        )
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_DEPENDENCY_REQUIRED"
        assert step["validation_error"]["field"] == "depends_on"

    @patch(_PATCH_TARGET)
    def test_empty_ref_in_request_profile_id_rejected(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(request_profile_id="$ref:")
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_PROFILE_REF_UNRESOLVED"

    @patch(_PATCH_TARGET)
    def test_payload_source_ref_missing_from_depends_on_marks_unresolvable(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(
            depends_on=("target_rest_connection", "target_json_profile"),
            payload_source_ref_key="missing_map",
        )
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_DEPENDENCY_REQUIRED"
        assert step["validation_error"]["field"] == "depends_on"

    @patch(_PATCH_TARGET)
    def test_raw_xml_rest_skips_builder_preflight(self, mock_pag):
        """config.xml is the raw-XML escape hatch — builder validation is bypassed."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="rest_raw", type="connector-settings", action="create",
            name="Raw REST",
            config={
                "connector_type": "rest",
                "xml": "<bns:Component>...pre-built XML...</bns:Component>",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_raw_xml_with_rest_subtype_and_no_connector_type_rejects_secret(self, mock_pag):
        """Regression: raw-XML REST components without connector_type still
        trigger the secret scan via subType inference (codex review item #2
        from the superseded HTTP implementation)."""
        mock_pag.return_value = []
        comp = IntegrationComponentSpec(
            key="rest_raw_leak", type="connector-settings", action="create",
            name="Raw REST",
            config={
                "xml": (
                    '<bns:Component type="connector-settings" '
                    'subType="officialboomi-X3979C-rest-prod">...</bns:Component>'
                ),
                "password": "DEADBEEF_RAW_REST",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "password"
        assert "DEADBEEF_RAW_REST" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_reuse_path_skips_rest_builder_preflight(self, mock_pag):
        mock_pag.return_value = [_meta(
            "existing-rest-id", "Target REST OAuth2 Connection", "Process Library",
            comp_type="connector-settings",
        )]
        comp = _rest_conn_comp()
        comp.config.pop("base_url")  # would fail builder validation
        plan = _build_plan(MagicMock(), _build_config([comp], conflict_policy="reuse"))
        step = plan["steps"][0]
        assert step["planned_action"] == "reuse"
        assert step["existing_component_id"] == "existing-rest-id"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_update_path_runs_rest_builder_preflight(self, mock_pag):
        """Codex r2 P2: REST connection updates with body fields invoke
        the builder at apply, so plan-time validation runs for update too."""
        comp = _rest_conn_comp(action="update")
        comp.component_id = "explicit-rest-id"
        comp.config.pop("base_url")
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        # base_url is required by the REST connection builder.
        assert "base_url" in step["validation_error"]["field"]

    @patch(_PATCH_TARGET)
    def test_unsupported_auth_marks_rest_connection_unresolvable(self, mock_pag):
        """CUSTOM / PASSWORD_DIGEST / AWS_SIGNATURE / AWS_IAM_ROLES_ANYWHERE
        remain unbuildable post-Phase 2. Verify the preflight catches one
        of them and surfaces UNSUPPORTED_REST_AUTH_MODE."""
        mock_pag.return_value = []
        cfg_overrides = {"auth": "PASSWORD_DIGEST"}
        comp = _rest_conn_comp(**cfg_overrides)
        comp.config.pop("oauth2", None)
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_REST_AUTH_MODE"

    @patch(_PATCH_TARGET)
    def test_unknown_method_marks_rest_operation_unresolvable(self, mock_pag):
        """Phase 5 made all 8 REST verbs buildable, so the prior
        UNVERIFIED_REST_XML_VARIANT path no longer fires for POST/PUT/etc.
        Replace it with a truly unknown method check — preflight must still
        catch garbage methods at plan time."""
        mock_pag.return_value = []
        op = _rest_op_comp(method="MAGIC")
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_REST_METHOD"


    @patch(_PATCH_TARGET)
    def test_all_eight_rest_methods_plan_clean(self, mock_pag):
        """Phase 5 sanity: each of the 8 buildable REST methods plans clean
        through preflight with otherwise-valid config."""
        mock_pag.return_value = []
        for method in ("GET", "PATCH", "PUT", "POST", "DELETE", "HEAD", "OPTIONS", "TRACE"):
            op = _rest_op_comp(method=method)
            components = [_rest_conn_comp(), *_rest_supporting_components(), op]
            plan = _build_plan(MagicMock(), _build_config(components))
            step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
            assert step["planned_action"] == "create", (
                f"Method {method} should plan as 'create' (no validation error). "
                f"Got planned_action={step.get('planned_action')!r}, "
                f"error={step.get('validation_error')}."
            )

    # ---- Plaintext-secret scan runs on EVERY REST step regardless of apply path.

    @patch(_PATCH_TARGET)
    def test_plaintext_secret_on_rest_connection_marks_unresolvable(self, mock_pag):
        mock_pag.return_value = []
        comp = _rest_conn_comp(password="DEADBEEF_REST_CONN")
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "password"
        assert "DEADBEEF_REST_CONN" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["password"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_plaintext_oauth2_client_secret_marks_unresolvable(self, mock_pag):
        """Regression: oauth2.client_secret must be rejected even though it's
        nested (codex review item #1 from the superseded HTTP implementation)."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["oauth2"]["client_secret"] = "DEADBEEF_OAUTH2_NESTED"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "oauth2.client_secret"
        assert "DEADBEEF_OAUTH2_NESTED" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_plaintext_secret_on_rest_operation_marks_unresolvable(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(token="DEADBEEF_REST_OP")
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        assert step["validation_error"]["field"] == "token"
        assert "DEADBEEF_REST_OP" not in repr(plan)

    # ---- Codex review fix: REST_SECRET_VALUE_FORBIDDEN / NEEDS_REST_EXAMPLE
    # ---- must also redact the offending field in the plan echo, not just
    # ---- PLAINTEXT_SECRET_REJECTED.

    @patch(_PATCH_TARGET)
    def test_raw_value_in_client_secret_ref_is_redacted_in_plan_echo(self, mock_pag):
        """REST_SECRET_VALUE_FORBIDDEN fires when oauth2.client_secret_ref
        carries a raw secret instead of a 'credential://...' opaque ref.
        The raw value must not appear in the plan response (integration_spec
        echo)."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["oauth2"]["client_secret_ref"] = "raw-leak-DEADBEEF_REF"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_SECRET_VALUE_FORBIDDEN"
        assert step["validation_error"]["field"] == "oauth2.client_secret_ref"
        assert "raw-leak-DEADBEEF_REF" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["oauth2"]["client_secret_ref"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_secret_shaped_request_header_redacted_in_plan_echo(self, mock_pag):
        """Phase 6: REST_SECRET_VALUE_FORBIDDEN fires for secret-shaped
        header keys (Authorization, X-API-Key, Bearer, etc.). The entire
        offending map must not leak through the plan echo — values may
        carry secrets even if the key itself triggered the rejection."""
        mock_pag.return_value = []
        op = _rest_op_comp(request_headers={"Authorization": "Bearer DEADBEEF_HDR"})
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_SECRET_VALUE_FORBIDDEN"
        assert step["validation_error"]["field"] == "request_headers"
        assert "DEADBEEF_HDR" not in repr(plan)
        echoed = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == "target_rest_operation"
        )["config"]
        assert echoed["request_headers"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_request_headers_redacted_when_earlier_validation_error_wins(self, mock_pag):
        """Codex round-6 P1: sensitive request_headers must be redacted
        from the plan echo even when an EARLIER validation error
        (REST_CONNECTION_REF_REQUIRED) wins over NEEDS_REST_EXAMPLE.
        Pre-fix, redaction only fired for the field named in the winning
        error code, so the operation's request_headers leaked when the
        connection check fired first."""
        mock_pag.return_value = []
        op = _rest_op_comp(
            depends_on=("target_json_profile", "payload_map"),
            request_headers={"Authorization": "Bearer LEAK_EARLIER_ERROR"},
        )
        op.config.pop("connection_ref_key")  # earlier error fires first
        components = [*_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        # connection-ref error wins (earliest in validate_config order).
        assert step["validation_error"]["error_code"] == "REST_CONNECTION_REF_REQUIRED"
        # But the sensitive header value MUST still be scrubbed from the echo.
        assert "LEAK_EARLIER_ERROR" not in repr(plan)
        echoed = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == "target_rest_operation"
        )["config"]
        assert echoed["request_headers"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_oauth2_client_secret_ref_raw_value_redacted_when_earlier_error_wins(self, mock_pag):
        """Codex round-6 P1: a raw value in oauth2.client_secret_ref must
        be scrubbed even when an earlier validator (e.g. missing base_url)
        wins over the REST_SECRET_VALUE_FORBIDDEN check."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["base_url"] = ""  # forces earlier REST_BASE_URL_REQUIRED
        comp.config["oauth2"]["client_secret_ref"] = "raw-LEAK_BEFORE_BASE_URL_CHECK"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "REST_BASE_URL_REQUIRED"
        assert "raw-LEAK_BEFORE_BASE_URL_CHECK" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["oauth2"]["client_secret_ref"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_stale_oauth2_block_on_non_oauth2_auth_redacts_secret(self, mock_pag):
        """Codex round-1 P1 #1: a non-OAUTH2 connection with a stale
        `oauth2` block carrying a raw client_secret_ref must (a) fail
        validation with REST_CONNECTOR_VALIDATION_FAILED field='oauth2',
        and (b) scrub the raw secret value from the plan echo via the
        existing _REST_SENSITIVE_FIELD_PATHS sweep."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config["oauth2"]["client_secret_ref"] = "raw-LEAK_CODEX_ROUND1_OAUTH2"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_CONNECTOR_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "oauth2"
        assert "raw-LEAK_CODEX_ROUND1_OAUTH2" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["oauth2"]["client_secret_ref"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_stale_credential_ref_on_non_password_auth_redacts_secret(self, mock_pag):
        """Codex round-1 P1 #2: a non-password connection (auth=NONE)
        with a stale `credential_ref` carrying a raw secret must (a) fail
        validation with REST_CONNECTOR_VALIDATION_FAILED field='credential_ref',
        and (b) scrub the raw value from the plan echo. The default fixture
        carries an oauth2 block; remove it so the credential_ref gate fires
        instead of the stale-oauth2 gate (which runs first by design)."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config.pop("oauth2", None)
        comp.config["credential_ref"] = "raw-LEAK_CODEX_ROUND1_CREDREF"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_CONNECTOR_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "credential_ref"
        assert "raw-LEAK_CODEX_ROUND1_CREDREF" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["credential_ref"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_non_dict_oauth2_string_value_redacted_in_plan_echo(self, mock_pag):
        """QA Bug #126: `_redact_dotted_field_path` walks dotted paths and
        no-ops if an intermediate value is non-dict. A stale `oauth2="raw"`
        (string) would echo into the plan output because the existing
        sensitive paths assume a dict shape. Defense in depth: the redact
        helper should detect the non-dict intermediate and clear the
        top-level value instead."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config["oauth2"] = "raw-LEAK_BUG126_STRING_OAUTH2"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["field"] == "oauth2"
        assert "LEAK_BUG126_STRING_OAUTH2" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["oauth2"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_non_dict_oauth2_list_value_redacted_in_plan_echo(self, mock_pag):
        """QA Bug #126 list variant — same defense as the string case."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config["oauth2"] = ["raw-LEAK_BUG126_LIST_OAUTH2"]
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert "LEAK_BUG126_LIST_OAUTH2" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["oauth2"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_dict_oauth2_redaction_preserves_non_secret_fields(self, mock_pag):
        """Regression sanity: for the existing dict-shaped oauth2 case
        (which DOES support per-field redaction), `_redact_dotted_field_path`
        must still leave non-secret fields visible. Only `client_secret_ref`
        gets `[REDACTED]`; `grant_type` etc. survive."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["base_url"] = ""  # earlier validator wins
        comp.config["oauth2"]["client_secret_ref"] = "raw-LEAK_KEEP_DICT_PATH"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        echoed = plan["integration_spec"]["components"][0]["config"]
        # client_secret_ref redacted, but the surrounding oauth2 stays a dict
        # with grant_type / client_id / access_token_url still visible.
        assert echoed["oauth2"]["client_secret_ref"] == "[REDACTED]"
        assert echoed["oauth2"]["grant_type"] == "client_credentials"
        assert "raw-LEAK_KEEP_DICT_PATH" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_codex_round5_valid_cert_ref_preserved_on_unrelated_error(self, mock_pag):
        """Codex round-5 P2: when an UNRELATED REST validation error fires
        (e.g. missing base_url), a valid GUID-shaped cert ref must be
        preserved in the plan echo — otherwise the caller can't correct
        the spec from the returned output. Round-4 added cert refs to the
        always-redact list, which over-redacted valid GUIDs."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config.pop("oauth2", None)
        comp.config["base_url"] = ""  # forces REST_BASE_URL_REQUIRED
        comp.config["private_certificate_ref"] = "21f598a6-1d90-4578-a35a-d0350c50b747"
        comp.config["public_certificate_ref"] = "ea82aa0c-484b-40b1-890c-f142ab8fecad"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        # Different field fails — base_url, not the cert refs.
        assert step["validation_error"]["error_code"] == "REST_BASE_URL_REQUIRED"
        echoed = plan["integration_spec"]["components"][0]["config"]
        # Valid GUIDs MUST survive intact so the caller can fix base_url
        # and resubmit without re-entering the certificate binding.
        assert echoed["private_certificate_ref"] == "21f598a6-1d90-4578-a35a-d0350c50b747"
        assert echoed["public_certificate_ref"] == "ea82aa0c-484b-40b1-890c-f142ab8fecad"

    @patch(_PATCH_TARGET)
    def test_codex_round5_pem_cert_ref_still_redacted_on_unrelated_error(self, mock_pag):
        """Inverse sanity: if the cert ref carries PEM content (not GUID)
        AND an unrelated field fails first, the PEM material must STILL
        be scrubbed — shape-conditional redaction is asymmetric."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config.pop("oauth2", None)
        comp.config["base_url"] = ""  # earlier error wins
        comp.config["private_certificate_ref"] = (
            "-----BEGIN PRIVATE KEY-----\n"
            "PEMCANARY_R5_LEAK_TEST_DEADBEEF\n"
            "-----END PRIVATE KEY-----"
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "REST_BASE_URL_REQUIRED"
        echoed = plan["integration_spec"]["components"][0]["config"]
        # Non-GUID cert ref still redacted defensively.
        assert echoed["private_certificate_ref"] == "[REDACTED]"
        assert "PEMCANARY_R5_LEAK_TEST_DEADBEEF" not in repr(plan)

    @patch(_PATCH_TARGET)
    def test_codex_round4_pem_cert_ref_redacted_in_plan_echo(self, mock_pag):
        """Codex round-4 P1: when the GUID validator rejects PEM/key
        material supplied as a `private_certificate_ref`, the integration
        plan echo MUST scrub the field — otherwise the key material
        survives in `integration_spec.components[].config.private_certificate_ref`.
        Closes the round-4 leak path."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config.pop("oauth2", None)
        comp.config["private_certificate_ref"] = (
            "-----BEGIN PRIVATE KEY-----\n"
            "PEMCANARY_R4_PRIVATE_KEY_MATERIAL_DEADBEEF\n"
            "-----END PRIVATE KEY-----"
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_CONNECTOR_VALIDATION_FAILED"
        assert step["validation_error"]["field"] == "private_certificate_ref"
        # Canary key material MUST NOT appear anywhere in the plan repr.
        assert "PEMCANARY_R4_PRIVATE_KEY_MATERIAL_DEADBEEF" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["private_certificate_ref"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_codex_round4_pem_public_cert_ref_redacted_in_plan_echo(self, mock_pag):
        """Same defense for `public_certificate_ref`."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["auth"] = "NONE"
        comp.config.pop("oauth2", None)
        comp.config["public_certificate_ref"] = (
            "-----BEGIN CERTIFICATE-----\n"
            "PEMCANARY_R4_PUBLIC_CERT_MATERIAL_DEADBEEF\n"
            "-----END CERTIFICATE-----"
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["field"] == "public_certificate_ref"
        assert "PEMCANARY_R4_PUBLIC_CERT_MATERIAL_DEADBEEF" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["public_certificate_ref"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_credential_ref_redacted_when_earlier_error_wins(self, mock_pag):
        """Codex round-6 P1: credential_ref should be `credential://...`
        per design, but callers can mistakenly put a raw secret there.
        It must be scrubbed when any rest_err fires."""
        mock_pag.return_value = []
        op = _rest_op_comp(
            depends_on=("target_json_profile", "payload_map"),
            credential_ref="raw-LEAK_IN_CRED_REF",
        )
        op.config.pop("connection_ref_key")
        components = [*_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert "raw-LEAK_IN_CRED_REF" not in repr(plan)
        echoed = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == "target_rest_operation"
        )["config"]
        assert echoed["credential_ref"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_query_parameters_redacted_when_earlier_error_wins(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(
            depends_on=("target_json_profile", "payload_map"),
            query_parameters={"api_key": "LEAK_QP_EARLIER"},
        )
        op.config.pop("connection_ref_key")
        components = [*_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["validation_error"]["error_code"] == "REST_CONNECTION_REF_REQUIRED"
        assert "LEAK_QP_EARLIER" not in repr(plan)
        echoed = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == "target_rest_operation"
        )["config"]
        assert echoed["query_parameters"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_secret_shaped_query_parameter_redacted_in_plan_echo(self, mock_pag):
        """Phase 6: secret-shaped query parameter keys (api_key, token,
        password, etc.) fire REST_SECRET_VALUE_FORBIDDEN. The map is
        redacted from the plan echo so the offending value never reaches
        the caller's screen."""
        mock_pag.return_value = []
        op = _rest_op_comp(query_parameters={"api_key": "DEADBEEF_QP"})
        components = [_rest_conn_comp(), *_rest_supporting_components(), op]
        plan = _build_plan(MagicMock(), _build_config(components))
        step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "REST_SECRET_VALUE_FORBIDDEN"
        assert step["validation_error"]["field"] == "query_parameters"
        assert "DEADBEEF_QP" not in repr(plan)
        echoed = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == "target_rest_operation"
        )["config"]
        assert echoed["query_parameters"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_oauth2_authorization_parameters_redacted_on_validation_error(self, mock_pag):
        """Codex round-3 P1: a non-empty `oauth2.authorization_parameters`
        triggers UNSUPPORTED_REST_OAUTH2_PARAMETERS, but before the fix
        the rejected dict echoed into `integration_spec` verbatim because
        the path wasn't in `_REST_SENSITIVE_FIELD_PATHS`. Add the path so
        caller-supplied content (which may contain arbitrary user values)
        is scrubbed on the rejection path."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["oauth2"]["authorization_parameters"] = {
            "prompt": "LEAK_CODEX_R3_AUTH_PARAM_CANARY",
        }
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_REST_OAUTH2_PARAMETERS"
        assert step["validation_error"]["field"] == "oauth2.authorization_parameters"
        assert "LEAK_CODEX_R3_AUTH_PARAM_CANARY" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["oauth2"]["authorization_parameters"] == "[REDACTED]"

    @patch(_PATCH_TARGET)
    def test_oauth2_access_token_parameters_redacted_on_validation_error(self, mock_pag):
        """Codex round-3 P1 (companion): same leak path for
        `oauth2.access_token_parameters`."""
        mock_pag.return_value = []
        comp = _rest_conn_comp()
        comp.config["oauth2"]["access_token_parameters"] = [
            {"key": "audience", "value": "LEAK_CODEX_R3_TOKEN_PARAM_CANARY"},
        ]
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_rest_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_REST_OAUTH2_PARAMETERS"
        assert step["validation_error"]["field"] == "oauth2.access_token_parameters"
        assert "LEAK_CODEX_R3_TOKEN_PARAM_CANARY" not in repr(plan)
        echoed = plan["integration_spec"]["components"][0]["config"]
        assert echoed["oauth2"]["access_token_parameters"] == "[REDACTED]"


class TestApplyRestPreflight:
    """Issue #24 — error_rest_validation steps must block apply."""

    @patch(_PATCH_TARGET)
    def test_apply_bails_on_rest_validation_error(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(depends_on=("target_json_profile", "payload_map"))
        op.config.pop("connection_ref_key")
        components = [*_rest_supporting_components(), op]
        config = _build_config(components)
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert "unresolvable_steps" in result
        unresolvable_keys = [s["key"] for s in result["unresolvable_steps"]]
        assert "target_rest_operation" in unresolvable_keys

    @patch("src.boomi_mcp.categories.integration_builder.create_connector")
    @patch("src.boomi_mcp.categories.integration_builder._resolve_existing_components")
    @patch(_PATCH_TARGET)
    def test_apply_normalizes_rest_alias_for_boomi_get_connector_check(
        self, mock_pag, mock_resolve, mock_create_connector,
    ):
        """Regression for codex review P2: planning accepts the local alias
        connector_type='rest_client', but Boomi's get_connector API only
        knows the canonical subtype 'officialboomi-X3979C-rest-prod'. The
        apply path must normalize the alias before the sanity check so the
        connector type validation doesn't fail on a clean plan."""
        mock_pag.return_value = []
        mock_resolve.return_value = []
        mock_create_connector.return_value = {
            "_success": True,
            "component_id": "rest-conn-aliased-001",
            "type": "connector-settings",
            "sub_type": "officialboomi-X3979C-rest-prod",
        }

        boomi_client = MagicMock()
        # If the alias leaked through to get_connector, this mock would still
        # accept it — but the assertion below checks the exact call argument.
        boomi_client.connector.get_connector.return_value = MagicMock()

        comp = _rest_conn_comp(connector_type="rest_client")
        config = _build_config([comp])
        config["dry_run"] = False
        result = _apply_plan(boomi_client, "dev", config)

        assert result["_success"] is True
        # get_connector must be called with the canonical Boomi subtype, not
        # the local alias.
        get_connector_args = [
            call.args for call in boomi_client.connector.get_connector.call_args_list
        ]
        assert ("officialboomi-X3979C-rest-prod",) in get_connector_args
        assert ("rest_client",) not in get_connector_args
        # The payload handed to create_connector must also carry the canonical.
        create_call = mock_create_connector.call_args_list[0]
        applied_payload = create_call.args[2]
        assert applied_payload["connector_type"] == "officialboomi-X3979C-rest-prod"

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch("src.boomi_mcp.categories.integration_builder._resolve_existing_components")
    @patch(_PATCH_TARGET)
    def test_apply_resolves_request_profile_id_ref_at_apply_time(
        self, mock_pag, mock_resolve, mock_exec,
    ):
        """$ref:target_json_profile in request_profile_id must be substituted
        with the resolved component_id by _resolve_dependency_tokens before
        the REST operation is executed."""
        mock_pag.return_value = []
        mock_resolve.return_value = []
        component_ids = {
            "target_rest_connection": "conn-r001",
            "target_json_profile": "prof-r001",
            "payload_map": "map-r001",
            "target_rest_operation": "op-r001",
        }

        def _mock_exec(*, comp, **_):
            return {
                "_success": True,
                "component_id": component_ids[comp.key],
                "type": comp.type,
            }

        mock_exec.side_effect = _mock_exec
        components = [
            _rest_conn_comp(),
            *_rest_supporting_components(),
            _rest_op_comp(),
        ]
        config = _build_config(components)
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        send_call = next(
            call for call in mock_exec.call_args_list
            if call.kwargs["comp"].key == "target_rest_operation"
        )
        resolved_config = send_call.kwargs["config"]
        assert resolved_config["request_profile_id"] == "prof-r001"


# ---------------------------------------------------------------------------
# Issue #25 — process-flow builder (M2.5)
# ---------------------------------------------------------------------------

def _process_flow_config(**overrides):
    """A minimal-valid database_to_api_sync process-flow config."""
    cfg = {
        "process_kind": "database_to_api_sync",
        "source": {
            "connector_type": "database",
            "connection_id": "$ref:db_connection",
            "operation_id": "$ref:db_query_operation",
            "action_type": "Get",
        },
        "transform": {"mode": "passthrough"},
        "target": {
            "connector_type": "rest",
            "connection_id": "$ref:target_rest_connection",
            "operation_id": "$ref:target_rest_operation",
            "action_type": "POST",
        },
        "reliability": {"retry_count": 0, "dlq": {"mode": "disabled"}},
    }
    cfg.update(overrides)
    return cfg


def _process_flow_comp(
    key="main_process",
    name="Main Process",
    action="create",
    depends_on=(
        "db_connection",
        "db_query_operation",
        "target_rest_connection",
        "target_rest_operation",
    ),
    **config_overrides,
):
    return IntegrationComponentSpec(
        key=key,
        type="process",
        action=action,
        name=name,
        config=_process_flow_config(**config_overrides),
        depends_on=list(depends_on),
    )


# Standard process-flow dep keys map to specific cross-component roles
# (issue #49). When _stub_dep_comp is called with one of these keys the
# stub is typed to match that role so the new PROCESS_REF_TYPE_MISMATCH
# check at plan-time treats it as the right shape.
_STUB_DEP_ROLES = {
    "db_connection": "database connector-settings",
    "db_query_operation": "database connector-action Get",
    "target_rest_connection": "REST Client connector-settings",
    "target_rest_operation": "REST Client connector-action",
    # Issue #51: DLQ catch-leg ref targets.
    "dlq_document_cache": "Document Cache",
    "dlq_error_subprocess": "error subprocess",
}


def _stub_dep_comp(key, *, role=None, method="POST"):
    """Role-aware stub to satisfy depends_on without dragging in real builders.

    For the four standard process-flow dep keys (see _STUB_DEP_ROLES), the
    stub takes the matching connector / action shape so issue #49 cross-component
    $ref type validation accepts it. For ad-hoc keys, falls back to a
    profile.json wrapper (the original semantics).

    action="update" plus a synthetic component_id keeps the stub on the
    update path so plan-time builder validation (which would reject these
    minimal configs as malformed) is skipped — the DB/REST builder
    validate_config blocks only run on create/create_clone.
    """
    role = role or _STUB_DEP_ROLES.get(key, "profile.json")
    name = key.replace("_", " ").title()
    stub_id = "00000000-0000-0000-0000-stubbed00001"
    if role == "database connector-settings":
        return IntegrationComponentSpec(
            key=key, type="connector-settings", action="update",
            component_id=stub_id, name=name,
            config={"connector_type": "database", "name": name},
        )
    if role == "database connector-action Get":
        return IntegrationComponentSpec(
            key=key, type="connector-action", action="update",
            component_id=stub_id, name=name,
            config={"connector_type": "database", "operation_mode": "get", "name": name},
        )
    if role == "REST Client connector-settings":
        return IntegrationComponentSpec(
            key=key, type="connector-settings", action="update",
            component_id=stub_id, name=name,
            config={"connector_type": "rest", "name": name},
        )
    if role == "REST Client connector-action":
        # Codex r2 P2 follow-up: structured connector-action updates now
        # invoke the builder at apply time, so the stub config must be
        # builder-valid (path + connection_ref_key required). The
        # cross-ref check (issue #49) reads `method` for action_type
        # matching.
        return IntegrationComponentSpec(
            key=key, type="connector-action", action="update",
            component_id=stub_id, name=name,
            config={
                "connector_type": "rest",
                "operation_mode": "execute",
                "method": method,
                "path": "/v1/stub",
                "component_name": name,
                "connection_ref_key": "target_rest_connection",
            },
            depends_on=["target_rest_connection"],
        )
    if role == "Document Cache":
        # Issue #51: a Document Cache catch-leg target. reference_only-style
        # (action="update" + component_id) so it is never built; documentcache
        # has no create path. _effective_component_type returns "documentcache".
        return IntegrationComponentSpec(
            key=key, type="documentcache", action="update",
            component_id=stub_id, name=name,
            config={"name": name},
        )
    if role == "error subprocess":
        # Issue #51: a process/subprocess catch-leg target. A real typed
        # process (a minimal wrapper_subprocess calling an out-of-spec process
        # by literal id) so it plans clean as its own step under the
        # process_kind-required contract; _effective_component_type -> "process"
        # keeps the DLQ error_subprocess_ref type-check satisfied.
        return IntegrationComponentSpec(
            key=key, type="process", action="update",
            component_id=stub_id, name=name,
            config={
                "process_kind": "wrapper_subprocess",
                "process_calls": [
                    {"process_id": "99999999-9999-9999-9999-999999999999"}
                ],
            },
        )
    if role == "profile.db":
        return IntegrationComponentSpec(
            key=key, type="profile.db", action="update",
            component_id=stub_id, name=name,
            config={"name": name},
        )
    # Default: profile.json wrapper (original _stub_dep_comp semantics).
    return IntegrationComponentSpec(
        key=key, type="component", action="create",
        name=name, config={"name": key, "type": "profile.json"},
    )


class TestBuildPlanProcessFlow:
    """Plan-time validation for structured process-flow components."""

    @patch(_PATCH_TARGET)
    def test_valid_process_flow_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        assert plan["_success"] is True
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "create"
        assert process_step["route"] == "process_flow_xml"
        assert "validation_error" not in process_step

    @patch(_PATCH_TARGET)
    def test_topo_order_places_process_after_deps(self, mock_pag):
        mock_pag.return_value = []
        # Put the process FIRST in the spec to confirm topo sort still
        # moves it last in execution_order.
        components = [
            _process_flow_comp(),
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        order = plan["execution_order"]
        assert order.index("main_process") == len(order) - 1
        for dep in ("db_connection", "db_query_operation",
                    "target_rest_connection", "target_rest_operation"):
            assert order.index(dep) < order.index("main_process")

    @patch(_PATCH_TARGET)
    def test_unknown_process_kind_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(process_kind="not_a_real_archetype"),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_KIND_UNSUPPORTED"

    @patch(_PATCH_TARGET)
    def test_missing_ref_in_depends_on_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        # depends_on omits target_rest_operation, but config still
        # carries $ref:target_rest_operation.
        bad_process = _process_flow_comp(
            depends_on=("db_connection", "db_query_operation", "target_rest_connection"),
        )
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            bad_process,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "MISSING_PROCESS_DEPENDENCY"

    @patch(_PATCH_TARGET)
    def test_retry_count_positive_errors_with_unverified(self, mock_pag):
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(reliability={"retry_count": 1}),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_RETRY_UNVERIFIED"

    @patch(_PATCH_TARGET)
    def test_dlq_document_cache_missing_binding_errors_at_plan(self, mock_pag):
        # Issue #51 M3.R1a: document_cache_ref with retry_count == 0 is now
        # un-gated, but a missing cache binding is rejected as
        # PROCESS_DLQ_BINDING_INVALID (was PROCESS_RETRY_UNVERIFIED).
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(
                reliability={"retry_count": 0, "dlq": {"mode": "document_cache_ref"}},
            ),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_DLQ_BINDING_INVALID"

    @patch(_PATCH_TARGET)
    def test_dlq_document_cache_with_binding_plans_create(self, mock_pag):
        # Issue #51 M3.R1a: retry_count == 0 + a bound document_cache_ref DLQ
        # now plans cleanly (the verified Try/Catch wrapper is emitted at apply).
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(
                reliability={
                    "retry_count": 0,
                    "dlq": {
                        "mode": "document_cache_ref",
                        "document_cache_id": "99999999-9999-9999-9999-999999999999",
                    },
                },
            ),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "create"

    @patch(_PATCH_TARGET)
    def test_catch_notify_with_wired_dlq_plans_create(self, mock_pag):
        # Issue #89: a hand-authored process with a wired DLQ + valid catch_notify
        # plans cleanly.
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(
                reliability={
                    "retry_count": 0,
                    "dlq": {
                        "mode": "document_cache_ref",
                        "document_cache_id": "99999999-9999-9999-9999-999999999999",
                    },
                    "catch_notify": {
                        "level": "ERROR",
                        "message_template": "failed: meta.base.catcherrorsmessage",
                    },
                },
            ),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "create"

    @patch(_PATCH_TARGET)
    def test_invalid_catch_notify_errors_at_plan(self, mock_pag):
        # Issue #89: a catch_notify whose template omits the caught-error token
        # is rejected at plan time with PROCESS_NOTIFY_CONFIG_INVALID.
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(
                reliability={
                    "retry_count": 0,
                    "dlq": {
                        "mode": "document_cache_ref",
                        "document_cache_id": "99999999-9999-9999-9999-999999999999",
                    },
                    "catch_notify": {"level": "ERROR", "message_template": "no token"},
                },
            ),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_NOTIFY_CONFIG_INVALID"

    @patch(_PATCH_TARGET)
    def test_plaintext_secret_blocks_process_validation(self, mock_pag):
        import json as _json

        mock_pag.return_value = []
        bad = _process_flow_comp()
        bad.config["password"] = "hunter2"
        # Add a nested secret too — the redactor must descend into
        # arbitrary dict/list nesting, not just top-level keys.
        bad.config["source"]["api_key"] = "sk-leak-me"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        # The actual plaintext values must NOT survive into the plan
        # response. Redaction happens via comp.config mutation, which
        # propagates through spec.model_dump(). Codex review C1.
        serialized = _json.dumps(plan)
        assert "hunter2" not in serialized
        assert "sk-leak-me" not in serialized
        assert "[REDACTED]" in serialized

    @patch(_PATCH_TARGET)
    def test_process_without_kind_errors_process_kind_required(self, mock_pag):
        # Legacy freeform process JSON authoring has been removed: a process
        # component without config.process_kind is rejected at plan time.
        mock_pag.return_value = []
        untyped_process = IntegrationComponentSpec(
            key="untyped_proc",
            type="process",
            action="create",
            name="Untyped Process",
            config={
                "name": "Untyped Process",
                "shapes": [
                    {"type": "start", "name": "start"},
                    {"type": "stop", "name": "stop"},
                ],
            },
        )
        plan = _build_plan(MagicMock(), _build_config([untyped_process]))
        step = next(s for s in plan["steps"] if s["key"] == "untyped_proc")
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_KIND_REQUIRED"
        assert step["validation_error"]["field"] == "config.process_kind"

    @patch(_PATCH_TARGET)
    def test_process_without_name_errors_at_plan_create(self, mock_pag):
        """Codex review r6 P2.1: structured process_kind components require
        an explicit display name. Falling back to comp.key would land in
        Boomi as a process named after the user's internal dependency
        token. Reject at plan-time for create..."""
        mock_pag.return_value = []
        unnamed = _process_flow_comp()
        unnamed.name = None
        unnamed.config.pop("name", None)
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            unnamed,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_NAME_REQUIRED"
        assert process_step["validation_error"]["field"] == "name"

    @patch(_PATCH_TARGET)
    def test_process_without_name_errors_at_plan_update(self, mock_pag):
        """...and on update (which is the variant the reviewer's repro hit —
        update_component({xml:...}) is a full-XML replacement, so a missing
        name silently renames the existing process to comp.key)."""
        mock_pag.return_value = []
        unnamed_update = _process_flow_comp(action="update")
        unnamed_update.name = None
        unnamed_update.component_id = "00000000-0000-0000-0000-000000000099"
        unnamed_update.config.pop("name", None)
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            unnamed_update,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_NAME_REQUIRED"

    @patch(_PATCH_TARGET)
    def test_process_with_only_config_name_passes(self, mock_pag):
        """Caller can supply name via config.name even if comp.name is None
        (the IntegrationSpecV1 schema permits both surfaces)."""
        mock_pag.return_value = []
        comp = _process_flow_comp()
        comp.name = None
        comp.config["name"] = "Process Name From Config"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            comp,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "create"
        assert "validation_error" not in process_step

    @patch(_PATCH_TARGET)
    def test_name_mismatch_between_top_level_and_config_errors_at_plan(self, mock_pag):
        """Codex review r8 F1: planning + collision lookup use comp.name
        (the top-level field), but apply-time build() prefers
        payload["name"] (== config["name"]) to honor the r3 clone-suffix
        precedence. When both surfaces are set and disagree, the emitted
        XML carries a name that didn't go through collision lookup —
        creating duplicates or silently renaming on update."""
        mock_pag.return_value = []
        comp = _process_flow_comp(name="Top Level Name")
        comp.config["name"] = "Different Config Name"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            comp,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_NAME_CONFLICT"
        assert process_step["validation_error"]["field"] == "name"

    @patch(_PATCH_TARGET)
    def test_whitespace_only_name_difference_is_canonicalized(self, mock_pag):
        """Codex review r10: r8's PROCESS_NAME_CONFLICT compared stripped
        values, so top-level 'Name' + config.name ' Name ' looked equal
        — but _resolve_existing_components used raw 'Name' for lookup
        and emission used raw ' Name '. Result: plan said create, build
        emitted whitespace-padded XML, Boomi created a duplicate.
        Strip both surfaces at normalize time so plan + emit see the
        same canonical value."""
        # Match an existing "Name" in metadata
        mock_pag.return_value = [
            _meta("existing-proc-id", "Name", folder_name="X", comp_type="process"),
        ]
        # Construct a raw component dict (not via _process_flow_comp,
        # so we exercise _normalize_component end-to-end).
        proc = {
            "key": "main_process",
            "type": "process",
            "action": "create",
            "name": "Name",
            "depends_on": [
                "db_connection", "db_query_operation",
                "target_rest_connection", "target_rest_operation",
            ],
            "config": {
                "name": " Name ",  # whitespace-padded
                "process_kind": "database_to_api_sync",
                "source": {
                    "connector_type": "database", "connection_id": "C1",
                    "operation_id": "O1", "action_type": "Get",
                },
                "target": {
                    "connector_type": "rest", "connection_id": "C2",
                    "operation_id": "O2", "action_type": "POST",
                },
            },
        }
        stubs = [
            _stub_dep_comp("db_connection").model_dump(),
            _stub_dep_comp("db_query_operation").model_dump(),
            _stub_dep_comp("target_rest_connection").model_dump(),
            _stub_dep_comp("target_rest_operation").model_dump(),
        ]
        plan = _build_plan(
            MagicMock(),
            {"conflict_policy": "reuse", "integration_spec": {
                "version": "1.0", "name": "t", "components": stubs + [proc],
            }},
        )
        proc_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        # Canonicalized name → collision lookup finds the existing
        # process → planned_action is reuse, not create.
        assert proc_step["planned_action"] == "reuse"
        assert proc_step["existing_component_id"] == "existing-proc-id"
        # And the step's recorded name reflects the canonical form.
        assert proc_step["name"] == "Name"

    @patch(_PATCH_TARGET)
    def test_matching_top_level_and_config_name_passes(self, mock_pag):
        """Both surfaces set, identical → no conflict (regression guard)."""
        mock_pag.return_value = []
        comp = _process_flow_comp(name="Same Name")
        comp.config["name"] = "Same Name"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            comp,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "create"
        assert "validation_error" not in process_step

    @patch(_PATCH_TARGET)
    def test_unsupported_process_kind_caught_on_reuse_path(self, mock_pag):
        """Codex review r9: PROCESS_KIND_UNSUPPORTED must fire on every
        planned_action, not just mutating ones. When conflict_policy=reuse
        finds an existing same-named process, planned_action becomes
        'reuse' — the previous gate skipped the enum check entirely, so
        a typo like process_kind='totally_not_a_real_kind' planned clean
        as reuse instead of surfacing the contract error."""
        mock_pag.return_value = [
            _meta("existing-proc-id", "Existing Process",
                  folder_name="X", comp_type="process"),
        ]
        bad_kind = _process_flow_comp(
            name="Existing Process",
            process_kind="totally_not_a_real_kind",
        )
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad_kind,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        # Without the r9 fix this would be planned_action="reuse" with no error.
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_KIND_UNSUPPORTED"

    @patch(_PATCH_TARGET)
    def test_config_name_participates_in_collision_lookup(self, mock_pag):
        """Codex review r7 P2.1: with the r6 fix accepting config.name as
        a valid display name, collision detection has to see it too. If
        we only checked comp.name, a process whose name lives in config
        would dodge _resolve_existing_components and apply would create
        a duplicate (or surface a late API error under conflict_policy=fail).
        The promotion in _normalize_component closes that gap."""
        mock_pag.return_value = [
            _meta("existing-proc-id", "Process Name From Config",
                  folder_name="X", comp_type="process"),
        ]
        comp = _process_flow_comp()
        comp.name = None
        comp.config["name"] = "Process Name From Config"
        plan = _build_plan(MagicMock(), _build_config([
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            comp,
        ]))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        # With config.name promoted to top-level, the existing component
        # is found and conflict_policy=reuse should route to reuse.
        assert process_step["planned_action"] == "reuse"
        assert process_step["existing_component_id"] == "existing-proc-id"

    @patch(_PATCH_TARGET)
    def test_update_with_invalid_config_errors_at_plan(self, mock_pag):
        """Codex review C2: process update re-invokes the builder via
        update_component({"xml": ...}). Validation must run for update too,
        not just create/create_clone, or malformed update configs slip
        past plan-time and explode at apply."""
        mock_pag.return_value = []
        bad_update = _process_flow_comp(
            action="update",
        )
        bad_update.component_id = "00000000-0000-0000-0000-000000000099"
        # Knock out a required binding so validate_config rejects it.
        del bad_update.config["source"]["operation_id"]
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad_update,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_CONNECTOR_BINDING_INVALID"
        assert process_step["validation_error"]["field"] == "source.operation_id"

    @patch(_PATCH_TARGET)
    def test_non_string_process_kind_does_not_crash_route_selection(self, mock_pag):
        """QA bug #128: route selection in _build_plan normalizes process_kind
        BEFORE the builder validator runs. Non-string values used to raise
        AttributeError on .strip() at that site. Must now coerce via str()
        and surface a structured PROCESS_KIND_UNSUPPORTED instead."""
        mock_pag.return_value = []
        bad = _process_flow_comp()
        bad.config["process_kind"] = 123  # non-string
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_KIND_UNSUPPORTED"

    @patch(_PATCH_TARGET)
    def test_process_kind_plus_raw_xml_errors_at_plan(self, mock_pag):
        """Codex review C4: declaring both process_kind and a raw config.xml
        override used to silently drop the xml. Now rejected at plan-time
        with PROCESS_KIND_XML_CONFLICT so the caller picks one path."""
        mock_pag.return_value = []
        conflicted = _process_flow_comp()
        conflicted.config["xml"] = "<bns:Component/>"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            conflicted,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PROCESS_KIND_XML_CONFLICT"
        assert process_step["validation_error"]["field"] == "config.xml"

    @patch(_PATCH_TARGET)
    def test_process_kind_xml_secret_combo_redacts_secret(self, mock_pag):
        """Codex review r2 Q3: when process_kind + xml + secret are all set,
        the xml-conflict check used to short-circuit before the secret scan,
        leaving the plaintext value in raw_config (== comp.config) so it
        echoed through spec.model_dump(). Scan must run first and redact."""
        import json as _json

        mock_pag.return_value = []
        triple_threat = _process_flow_comp()
        triple_threat.config["xml"] = "<bns:Component/>"
        triple_threat.config["password"] = "hunter2"
        triple_threat.config["source"]["api_key"] = "sk-also-leaks"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            triple_threat,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        # Secret scan runs first → PLAINTEXT_SECRET_REJECTED wins.
        assert process_step["planned_action"] == "error_process_validation"
        assert process_step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        serialized = _json.dumps(plan)
        assert "hunter2" not in serialized
        assert "sk-also-leaks" not in serialized
        assert "[REDACTED]" in serialized


class TestApplyPlanProcessFlow:
    """Apply-time behavior for structured process-flow components."""

    @patch(_PATCH_TARGET)
    def test_apply_aborts_when_process_flow_validation_fails(self, mock_pag):
        mock_pag.return_value = []
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(process_kind="not_a_real_archetype"),
        ]
        config = _build_config(components)
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert "Plan contains unresolvable steps" in result["error"]
        # The unresolvable_steps envelope must surface the structured error.
        unresolvable_codes = {
            s["validation_error"]["error_code"]
            for s in result["unresolvable_steps"]
            if s.get("validation_error")
        }
        assert "PROCESS_KIND_UNSUPPORTED" in unresolvable_codes

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_resolves_refs_before_process_flow_build(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        component_ids = {
            "db_connection": "db-conn-uuid",
            "db_query_operation": "db-op-uuid",
            "target_rest_connection": "rest-conn-uuid",
            "target_rest_operation": "rest-op-uuid",
            "main_process": "proc-uuid",
        }

        def _mock_exec(*, comp, **_):
            return {
                "_success": True,
                "component_id": component_ids[comp.key],
                "type": comp.type,
            }

        mock_exec.side_effect = _mock_exec
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _process_flow_comp(),
        ]
        config = _build_config(components)
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True
        process_call = next(
            call for call in mock_exec.call_args_list
            if call.kwargs["comp"].key == "main_process"
        )
        resolved_config = process_call.kwargs["config"]
        # Every $ref token must be substituted with the real component id
        # by _resolve_dependency_tokens before _execute_component sees it.
        assert resolved_config["source"]["connection_id"] == "db-conn-uuid"
        assert resolved_config["source"]["operation_id"] == "db-op-uuid"
        assert resolved_config["target"]["connection_id"] == "rest-conn-uuid"
        assert resolved_config["target"]["operation_id"] == "rest-op-uuid"

    @patch(_PATCH_TARGET)
    def test_clone_policy_emits_suffixed_name_for_process_flow(self, mock_pag):
        """Codex review r3 P2 (clone bypass): _apply_clone_suffix writes
        '<name>-clone' into config['name'], but _execute_component used to
        consult comp.name first so the suffix was dropped. The emitted
        process Component XML must carry the suffixed name."""
        from src.boomi_mcp.categories.components.builders import ProcessFlowBuilder
        # Re-create the apply-time path manually so we can assert on the
        # emitted XML name. The real call chain runs _apply_clone_suffix
        # then _execute_component → ProcessFlowBuilder.build, but mocking
        # _execute_component would obscure exactly what we want to test.
        from src.boomi_mcp.categories.integration_builder import _apply_clone_suffix
        comp = _process_flow_comp(name="Main Process")
        # Build the resolved config exactly as _apply_clone_suffix would
        # leave it for an apply-time clone path.
        suffixed_config = _apply_clone_suffix(comp, dict(comp.config))
        suffixed_config["name"] = "Main Process-clone"  # matches helper

        xml = ProcessFlowBuilder.build(
            suffixed_config,
            name=suffixed_config.get("name") or comp.name or comp.key,
            folder_name=suffixed_config.get("folder_name"),
        )
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml)
        assert root.attrib["name"] == "Main Process-clone", (
            "clone-suffix must reach the emitted XML; comp.name should not "
            "override payload['name']"
        )


# ---------------------------------------------------------------------------
# Issue #49: cross-component $ref type validation
# ---------------------------------------------------------------------------


def _db_read_profile_stub(key="db_read_profile"):
    """profile.db stub usable as a read_profile_id $ref target."""
    return _stub_dep_comp(key, role="profile.db")


def _rest_json_profile_stub(key="target_json_profile"):
    """profile.json stub usable as a REST request/response_profile_id target.

    Same shape as the legacy default _stub_dep_comp produced — the
    fallback profile.json wrapper is already typed correctly for this
    role so no extra plumbing is needed.
    """
    return _stub_dep_comp(key, role="profile.json")


class TestBuildPlanDatabaseGetRefTypes:
    """Issue #49: typed $ref validation for database Get operations."""

    @patch(_PATCH_TARGET)
    def test_connection_ref_key_pointing_to_profile_returns_type_mismatch(self, mock_pag):
        mock_pag.return_value = []
        # connection_ref_key points at a profile.db component, not a
        # database connector-settings. Existing reachability check
        # passes (the key is in depends_on); new type check fires.
        op = _db_get_op_comp(connection_ref_key="db_read_profile")
        op.depends_on = ["db_read_profile"]
        op.config.pop("read_profile_id", None)
        plan = _build_plan(MagicMock(), _build_config([
            _db_read_profile_comp(),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        ve = op_step["validation_error"]
        assert op_step["planned_action"] == "error_database_validation"
        # read_profile_id is missing → that fires before the type check.
        # Restore read_profile_id and ensure connection_ref_key type-mismatch wins.
        assert ve["error_code"] == "MISSING_DB_READ_PROFILE_REF"

    @patch(_PATCH_TARGET)
    def test_connection_ref_key_pointing_to_action_returns_type_mismatch(self, mock_pag):
        mock_pag.return_value = []
        # Reuse the read-profile fixture but call the dep a connector-action stub.
        # connection_ref_key="rogue_action" → it's in depends_on but it's a
        # database connector-action, not connector-settings.
        bad_dep = _stub_dep_comp("rogue_action", role="database connector-action Get")
        op = _db_get_op_comp(connection_ref_key="rogue_action")
        op.depends_on = ["rogue_action", "db_read_profile"]
        plan = _build_plan(MagicMock(), _build_config([
            bad_dep,
            _db_read_profile_comp(),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        ve = op_step["validation_error"]
        assert op_step["planned_action"] == "error_database_validation"
        assert ve["error_code"] == "DB_REF_TYPE_MISMATCH"
        assert ve["field"] == "connection_ref_key"
        assert ve["details"]["ref_key"] == "rogue_action"
        assert ve["details"]["expected_role"] == "database connector-settings"
        assert "connector-action" in ve["details"]["actual_role"]

    @patch(_PATCH_TARGET)
    def test_read_profile_id_ref_to_connection_returns_type_mismatch(self, mock_pag):
        mock_pag.return_value = []
        # read_profile_id="$ref:db_connection" — pointing at the DB
        # connector-settings instead of a profile.db. Type check fires.
        op = _db_get_op_comp(read_profile_id="$ref:db_connection")
        op.depends_on = ["db_connection"]
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        ve = op_step["validation_error"]
        assert op_step["planned_action"] == "error_database_validation"
        assert ve["error_code"] == "DB_REF_TYPE_MISMATCH"
        assert ve["field"] == "read_profile_id"
        assert ve["details"]["expected_role"] == "profile.db"
        # actual_role reflects the database connector-settings classification.
        assert "database connector-settings" in ve["details"]["actual_role"]

    @patch(_PATCH_TARGET)
    def test_uuid_read_profile_id_skips_type_check(self, mock_pag):
        mock_pag.return_value = []
        op = _db_get_op_comp(read_profile_id="abc-123-def")
        op.depends_on = ["db_connection"]
        plan = _build_plan(MagicMock(), _build_config([
            _db_comp(),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        # Direct UUID is outside-spec; existing tests already confirm
        # this clean-plan path. Reasserted here for the issue #49 baseline.
        assert op_step.get("validation_error") is None

    @patch(_PATCH_TARGET)
    def test_missing_field_error_beats_type_check(self, mock_pag):
        # Regression: even when the wrong-type ref is present, the
        # pre-existing missing connection_ref_key check must still fire
        # first with its original error code. Issue #49 added type checks
        # AFTER existing ones — we don't want to reorder error_code
        # ordering for callers depending on stable validation surface.
        mock_pag.return_value = []
        op = _db_get_op_comp()
        op.config.pop("connection_ref_key")
        op.depends_on = ["db_read_profile"]
        plan = _build_plan(MagicMock(), _build_config([
            _db_read_profile_comp(),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "db_query_operation")
        ve = op_step["validation_error"]
        assert ve["error_code"] == "MISSING_DB_DEPENDENCY"
        assert ve["field"] == "connection_ref_key"
        # No details for the existing missing-field check (issue #49 only
        # added details to the new TYPE_MISMATCH codes).
        assert "details" not in ve


class TestBuildPlanRestRefTypes:
    """Issue #49: typed $ref validation for REST Client operations."""

    @patch(_PATCH_TARGET)
    def test_connection_ref_key_pointing_to_profile_returns_type_mismatch(self, mock_pag):
        mock_pag.return_value = []
        # connection_ref_key="target_json_profile" — the dep is a profile,
        # not a REST connector-settings. depends_on already includes it.
        op = _rest_op_comp(
            connection_ref_key="target_json_profile",
            depends_on=("target_json_profile", "payload_map"),
        )
        # payload_source_ref_key still references payload_map; supply a stub.
        plan = _build_plan(MagicMock(), _build_config([
            _rest_json_profile_stub("target_json_profile"),
            _stub_dep_comp("payload_map"),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        ve = op_step["validation_error"]
        assert op_step["planned_action"] == "error_rest_validation"
        assert ve["error_code"] == "REST_REF_TYPE_MISMATCH"
        assert ve["field"] == "connection_ref_key"
        assert ve["details"]["ref_key"] == "target_json_profile"
        assert ve["details"]["expected_role"] == "REST Client connector-settings"
        assert "profile.json" in ve["details"]["actual_role"]

    @patch(_PATCH_TARGET)
    def test_request_profile_id_ref_to_connection_returns_type_mismatch(self, mock_pag):
        mock_pag.return_value = []
        # request_profile_id="$ref:target_rest_connection" — points at
        # the REST connector-settings instead of a profile.json/xml.
        op = _rest_op_comp(
            request_profile_id="$ref:target_rest_connection",
            depends_on=("target_rest_connection", "payload_map"),
        )
        plan = _build_plan(MagicMock(), _build_config([
            _rest_conn_comp(),
            _stub_dep_comp("payload_map"),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        ve = op_step["validation_error"]
        assert op_step["planned_action"] == "error_rest_validation"
        assert ve["error_code"] == "REST_REF_TYPE_MISMATCH"
        assert ve["field"] == "request_profile_id"
        assert ve["details"]["expected_role"] == "profile.json or profile.xml"
        assert "REST Client connector-settings" in ve["details"]["actual_role"]

    @patch(_PATCH_TARGET)
    def test_response_profile_id_ref_to_connection_returns_type_mismatch(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(
            response_profile_id="$ref:target_rest_connection",
            depends_on=("target_rest_connection", "target_json_profile", "payload_map"),
        )
        plan = _build_plan(MagicMock(), _build_config([
            _rest_conn_comp(),
            _rest_json_profile_stub("target_json_profile"),
            _stub_dep_comp("payload_map"),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        ve = op_step["validation_error"]
        assert ve["error_code"] == "REST_REF_TYPE_MISMATCH"
        assert ve["field"] == "response_profile_id"

    @patch(_PATCH_TARGET)
    def test_profile_xml_ref_is_accepted_for_rest(self, mock_pag):
        mock_pag.return_value = []
        # profile.xml is an explicitly-supported REST profile type per the
        # issue #49 plan; should plan cleanly.
        xml_profile_stub = _stub_dep_comp("target_xml_profile", role="profile.json")
        # Override to declare config.type=profile.xml on the wrapper.
        xml_profile_stub.config["type"] = "profile.xml"
        op = _rest_op_comp(
            request_profile_id="$ref:target_xml_profile",
            depends_on=("target_rest_connection", "target_xml_profile", "payload_map"),
        )
        plan = _build_plan(MagicMock(), _build_config([
            _rest_conn_comp(),
            xml_profile_stub,
            _stub_dep_comp("payload_map"),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        assert op_step.get("validation_error") is None

    @patch(_PATCH_TARGET)
    def test_uuid_profile_id_skips_type_check(self, mock_pag):
        mock_pag.return_value = []
        op = _rest_op_comp(
            request_profile_id="some-existing-profile-uuid",
            response_profile_id="another-existing-profile-uuid",
            depends_on=("target_rest_connection", "payload_map"),
        )
        plan = _build_plan(MagicMock(), _build_config([
            _rest_conn_comp(),
            _stub_dep_comp("payload_map"),
            op,
        ]))
        op_step = next(s for s in plan["steps"] if s["key"] == "target_rest_operation")
        # Direct UUIDs are outside-spec and skip type-check; no error.
        assert op_step.get("validation_error") is None


class TestBuildPlanProcessFlowRefTypes:
    """Issue #49: typed $ref validation for structured database_to_api_sync processes."""

    @patch(_PATCH_TARGET)
    def test_swapped_source_connection_and_operation_refs_repro(self, mock_pag):
        # The repro the issue calls out: someone wires source.connection_id
        # at the operation key and source.operation_id at the connection.
        # Both targets exist and both keys are in depends_on, so the
        # pre-existing reachability check passes — only the new type
        # check catches the bug.
        mock_pag.return_value = []
        bad = _process_flow_comp()
        bad.config["source"]["connection_id"] = "$ref:db_query_operation"
        bad.config["source"]["operation_id"] = "$ref:db_connection"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        ve = process_step["validation_error"]
        assert process_step["planned_action"] == "error_process_validation"
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "source.connection_id"
        assert ve["details"]["ref_key"] == "db_query_operation"
        assert ve["details"]["expected_role"] == "database connector-settings"
        assert "connector-action" in ve["details"]["actual_role"]

    @patch(_PATCH_TARGET)
    def test_source_operation_id_pointing_to_connection_errors(self, mock_pag):
        mock_pag.return_value = []
        bad = _process_flow_comp()
        bad.config["source"]["operation_id"] = "$ref:db_connection"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        ve = next(s for s in plan["steps"] if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "source.operation_id"
        assert ve["details"]["expected_role"] == "database connector-action Get"

    @patch(_PATCH_TARGET)
    def test_target_connection_id_pointing_to_rest_op_errors(self, mock_pag):
        mock_pag.return_value = []
        bad = _process_flow_comp()
        bad.config["target"]["connection_id"] = "$ref:target_rest_operation"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        ve = next(s for s in plan["steps"] if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "target.connection_id"
        assert ve["details"]["expected_role"] == "REST Client connector-settings"

    @patch(_PATCH_TARGET)
    def test_target_operation_id_pointing_to_rest_connection_errors(self, mock_pag):
        mock_pag.return_value = []
        bad = _process_flow_comp()
        bad.config["target"]["operation_id"] = "$ref:target_rest_connection"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        ve = next(s for s in plan["steps"] if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "target.operation_id"
        assert ve["details"]["expected_role"] == "REST Client connector-action"

    @patch(_PATCH_TARGET)
    def test_process_extensions_ref_to_non_connection_errors(self, mock_pag):
        # Issue #92: a process_extensions connection-override id pointing at an
        # in-spec non-connection component (here the DB Get operation, which is
        # in depends_on so reachability passes) must be rejected — otherwise it
        # would emit a ConnectionOverride against a connector-action.
        mock_pag.return_value = []
        bad = _process_flow_comp()
        bad.config["process_extensions"] = {
            "connections": [
                {
                    "connection_id": "$ref:db_query_operation",
                    "connector_type": "database",
                    "fields": [
                        {"id": "password", "label": "Password",
                         "xpath": "DatabaseConnectionSettings/@password"},
                    ],
                }
            ]
        }
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        ve = next(s for s in plan["steps"] if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "process_extensions.connections[0].connection_id"
        assert ve["details"]["expected_role"] == "connector-settings"

    @patch(_PATCH_TARGET)
    def test_process_extensions_ref_to_db_connection_ok(self, mock_pag):
        # The archetype's own shape: the override id is the DB connection ref —
        # a connector-settings — so the type-check passes.
        mock_pag.return_value = []
        good = _process_flow_comp()
        good.config["process_extensions"] = {
            "connections": [
                {
                    "connection_id": "$ref:db_connection",
                    "connector_type": "database",
                    "fields": [
                        {"id": "password", "label": "Password",
                         "xpath": "DatabaseConnectionSettings/@password"},
                    ],
                }
            ]
        }
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            good,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step.get("validation_error") is None

    @patch(_PATCH_TARGET)
    def test_target_action_type_method_mismatch_errors(self, mock_pag):
        mock_pag.return_value = []
        # Default stub method is POST. target.action_type="PATCH" on the
        # process config disagrees with the referenced operation's declared
        # HTTP method.
        bad = _process_flow_comp()
        bad.config["target"]["action_type"] = "PATCH"
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),  # method=POST
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        ve = next(s for s in plan["steps"] if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "target.action_type"
        assert ve["details"]["expected_role"] == "POST"
        assert ve["details"]["actual_role"] == "PATCH"

    @patch(_PATCH_TARGET)
    def test_uuid_refs_skip_type_check(self, mock_pag):
        mock_pag.return_value = []
        # Replace every ref with a UUID-like literal; the existing
        # MISSING_PROCESS_DEPENDENCY check ignores non-$ref strings, and
        # the new type check also skips them. depends_on may be empty.
        bad = _process_flow_comp(depends_on=())
        bad.config["source"]["connection_id"] = "11111111-1111-1111-1111-111111111111"
        bad.config["source"]["operation_id"] = "22222222-2222-2222-2222-222222222222"
        bad.config["target"]["connection_id"] = "33333333-3333-3333-3333-333333333333"
        bad.config["target"]["operation_id"] = "44444444-4444-4444-4444-444444444444"
        plan = _build_plan(MagicMock(), _build_config([bad]))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        # No validation error: direct UUIDs are out-of-spec and pass.
        assert process_step.get("validation_error") is None
        assert process_step["planned_action"] == "create"

    @patch(_PATCH_TARGET)
    def test_missing_dep_error_beats_type_check(self, mock_pag):
        # Regression: when depends_on is missing the key entirely, the
        # pre-existing MISSING_PROCESS_DEPENDENCY check must fire — not
        # the new PROCESS_REF_TYPE_MISMATCH. ProcessFlowBuilder.validate_config
        # runs first and only when it returns clean do we run the type check.
        mock_pag.return_value = []
        bad = _process_flow_comp(
            depends_on=("db_query_operation", "target_rest_connection", "target_rest_operation"),
        )
        # source.connection_id still references db_connection but the key
        # is not in depends_on.
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        ve = next(s for s in plan["steps"] if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "MISSING_PROCESS_DEPENDENCY"
        # No details — only the new TYPE_MISMATCH codes ship structured details.
        assert "details" not in ve

    @patch(_PATCH_TARGET)
    def test_no_method_declared_on_rest_op_skips_action_type_check(self, mock_pag):
        # When the referenced REST operation has no declared method,
        # target.action_type cannot be checked against it — silently skip.
        mock_pag.return_value = []
        no_method_rest = _stub_dep_comp("target_rest_operation")
        # Strip the method field from the stub config.
        no_method_rest.config.pop("method", None)
        bad = _process_flow_comp()  # target.action_type defaults to POST
        plan = _build_plan(MagicMock(), _build_config([
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            no_method_rest,
            bad,
        ]))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step.get("validation_error") is None
        assert process_step["planned_action"] == "create"

    @patch(_PATCH_TARGET)
    def test_clean_typed_refs_plan_without_error(self, mock_pag):
        # Sanity: all refs point at correctly-typed in-spec components,
        # method matches action_type → clean plan.
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),  # method=POST, matches action_type=POST
            _process_flow_comp(),
        ]))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step.get("validation_error") is None
        assert process_step["planned_action"] == "create"

    # Issue #51: the newly un-gated DLQ catch-leg $ref slots get the same
    # plan-time type discipline as source/target.
    @patch(_PATCH_TARGET)
    def test_dlq_document_cache_ref_wrong_type_errors(self, mock_pag):
        # document_cache_id $ref pointing at a REST connector-settings (not a
        # Document Cache) is caught at plan time.
        mock_pag.return_value = []
        bad = _process_flow_comp(reliability={
            "retry_count": 0,
            "dlq": {"mode": "document_cache_ref",
                    "document_cache_id": "$ref:target_rest_connection"},
        })
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        ve = next(s for s in _build_plan(MagicMock(), _build_config(components))["steps"]
                  if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "reliability.dlq.document_cache_id"
        assert ve["details"]["expected_role"] == "Document Cache"
        assert ve["details"]["actual_role"] == "REST Client connector-settings"

    @patch(_PATCH_TARGET)
    def test_dlq_error_subprocess_ref_wrong_type_errors(self, mock_pag):
        # process_id $ref pointing at a database connector-settings (not a
        # process) is caught at plan time.
        mock_pag.return_value = []
        bad = _process_flow_comp(reliability={
            "retry_count": 0,
            "dlq": {"mode": "error_subprocess_ref",
                    "process_id": "$ref:db_connection"},
        })
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            bad,
        ]
        ve = next(s for s in _build_plan(MagicMock(), _build_config(components))["steps"]
                  if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "reliability.dlq.process_id"
        assert ve["details"]["expected_role"] == "error subprocess"
        assert ve["details"]["actual_role"] == "database connector-settings"

    @patch(_PATCH_TARGET)
    def test_dlq_document_cache_ref_correct_type_plans_clean(self, mock_pag):
        # document_cache_id $ref pointing at a real Document Cache component
        # passes the type check and plans cleanly.
        mock_pag.return_value = []
        good = _process_flow_comp(
            depends_on=("db_connection", "db_query_operation",
                        "target_rest_connection", "target_rest_operation",
                        "dlq_document_cache"),
            reliability={"retry_count": 0,
                         "dlq": {"mode": "document_cache_ref",
                                 "document_cache_id": "$ref:dlq_document_cache"}},
        )
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _stub_dep_comp("dlq_document_cache"),
            good,
        ]
        process_step = next(s for s in _build_plan(MagicMock(), _build_config(components))["steps"]
                            if s["key"] == "main_process")
        assert process_step.get("validation_error") is None
        assert process_step["planned_action"] == "create"

    @patch(_PATCH_TARGET)
    def test_dlq_error_subprocess_ref_correct_type_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        good = _process_flow_comp(
            depends_on=("db_connection", "db_query_operation",
                        "target_rest_connection", "target_rest_operation",
                        "dlq_error_subprocess"),
            reliability={"retry_count": 0,
                         "dlq": {"mode": "error_subprocess_ref",
                                 "process_id": "$ref:dlq_error_subprocess"}},
        )
        components = [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _stub_dep_comp("dlq_error_subprocess"),
            good,
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        # No step may carry a validation error — in particular the referenced
        # in-spec error-subprocess must itself plan clean, not silently fail
        # the process_kind-required gate.
        assert all(s.get("validation_error") is None for s in plan["steps"]), [
            (s["key"], s.get("validation_error")) for s in plan["steps"]
            if s.get("validation_error") is not None
        ]
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step["planned_action"] == "create"

    @patch(_PATCH_TARGET)
    def test_published_schema_examples_compose_without_method_mismatch(self, mock_pag):
        """Regression for codex r1: the published rest.operation and
        database_to_api_sync schema examples must compose into a single
        valid spec — i.e. their HTTP verbs must agree. Without this guard
        a user/agent copying both examples wholesale gets a confusing
        PROCESS_REF_TYPE_MISMATCH at plan time despite following the docs.
        """
        from src.boomi_mcp.categories.meta_tools import (
            get_schema_template_action,
        )
        mock_pag.return_value = []
        rest_op_template = get_schema_template_action(
            resource_type="component",
            operation="create",
            component_type="connector-action",
            protocol="rest.operation",
        )
        process_template = get_schema_template_action(
            resource_type="process",
            operation="create",
            protocol="database_to_api_sync",
        )
        rest_op_example = rest_op_template["example"]
        process_example = process_template["example_component_spec"]
        rest_op_method = rest_op_example["config"]["method"]
        process_action_type = process_example["config"]["target"]["action_type"]
        assert rest_op_method.upper() == process_action_type.upper(), (
            f"rest.operation example method={rest_op_method!r} disagrees "
            f"with database_to_api_sync example target.action_type="
            f"{process_action_type!r} — copies of both examples will hit "
            f"PROCESS_REF_TYPE_MISMATCH at plan time"
        )


# ---------------------------------------------------------------------------
# Issue #26 — profile.json / profile.xml / transform.map plan routing
# ---------------------------------------------------------------------------


def _json_profile_comp(key="json_profile", name="Test JSON Profile", **config_overrides):
    cfg = {
        "component_type": "profile.json",
        "profile_type": "json.generated",
        "component_name": name,
        "root": {
            "name": "Root",
            "kind": "object",
            "children": [
                {"name": "a", "kind": "simple", "data_type": "character"},
                {"name": "list", "kind": "array", "children": [
                    {"name": "key", "kind": "simple", "data_type": "character"},
                ]},
            ],
        },
    }
    cfg.update(config_overrides)
    return IntegrationComponentSpec(
        key=key, type="profile.json", action="create", name=name, config=cfg,
    )


def _xml_profile_comp(key="xml_profile", name="Test XML Profile", **config_overrides):
    cfg = {
        "component_type": "profile.xml",
        "profile_type": "xml.generated",
        "component_name": name,
        "root": {
            "name": "rows",
            "kind": "element",
            "max_occurs": 1,
            "children": [
                {"name": "row", "kind": "element", "max_occurs": -1, "children": [
                    {"name": "key", "kind": "element", "data_type": "character"},
                ]},
            ],
        },
    }
    cfg.update(config_overrides)
    return IntegrationComponentSpec(
        key=key, type="profile.xml", action="create", name=name, config=cfg,
    )


def _direct_map_comp(
    key="json_to_json_map",
    name="Test Map",
    source_ref_key="json_profile",
    target_ref_key="json_profile",
    source_type="profile.json",
    target_type="profile.json",
    field_mappings=None,
    **config_overrides,
):
    cfg = {
        "component_type": "transform.map",
        "map_type": "direct",
        "component_name": name,
        "source_profile_id": f"$ref:{source_ref_key}",
        "source_profile_type": source_type,
        "target_profile_id": f"$ref:{target_ref_key}",
        "target_profile_type": target_type,
        "field_mappings": field_mappings or [
            {"source_path": "Root/a", "target_path": "Root/a"},
        ],
    }
    cfg.update(config_overrides)
    return IntegrationComponentSpec(
        key=key, type="transform.map", action="create", name=name, config=cfg,
        depends_on=[source_ref_key, target_ref_key] if source_ref_key != target_ref_key else [source_ref_key],
    )


class TestBuildPlanProcessFlowBranchRefTypes:
    """Issue #112 M10.8: typed $ref validation for Branch fan-out leg targets."""

    _BRANCH_DEPS = (
        "db_connection",
        "db_query_operation",
        "target_rest_connection",
        "target_rest_operation",
        "branch_leg_connection",
        "branch_leg_operation",
    )

    def _branch_components(self, leg, method="POST"):
        bad = _process_flow_comp(depends_on=self._BRANCH_DEPS)
        bad.config["branch"] = {"enabled": True, "targets": [leg]}
        return [
            _stub_dep_comp("db_connection"),
            _stub_dep_comp("db_query_operation"),
            _stub_dep_comp("target_rest_connection"),
            _stub_dep_comp("target_rest_operation"),
            _stub_dep_comp("branch_leg_connection", role="REST Client connector-settings"),
            _stub_dep_comp("branch_leg_operation", role="REST Client connector-action", method=method),
            bad,
        ]

    @staticmethod
    def _good_leg(**overrides):
        leg = {
            "connector_type": "rest",
            "action_type": "POST",
            "connection_id": "$ref:branch_leg_connection",
            "operation_id": "$ref:branch_leg_operation",
        }
        leg.update(overrides)
        return leg

    @patch(_PATCH_TARGET)
    def test_branch_leg_connection_id_pointing_to_rest_op_errors(self, mock_pag):
        mock_pag.return_value = []
        leg = self._good_leg(connection_id="$ref:branch_leg_operation")  # points at an op
        ve = next(s for s in _build_plan(MagicMock(), _build_config(self._branch_components(leg)))["steps"]
                  if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "branch.targets[0].connection_id"
        assert ve["details"]["expected_role"] == "REST Client connector-settings"

    @patch(_PATCH_TARGET)
    def test_branch_leg_operation_id_pointing_to_rest_connection_errors(self, mock_pag):
        mock_pag.return_value = []
        leg = self._good_leg(operation_id="$ref:branch_leg_connection")  # points at a connection
        ve = next(s for s in _build_plan(MagicMock(), _build_config(self._branch_components(leg)))["steps"]
                  if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "branch.targets[0].operation_id"
        assert ve["details"]["expected_role"] == "REST Client connector-action"

    @patch(_PATCH_TARGET)
    def test_branch_leg_action_type_method_mismatch_errors(self, mock_pag):
        mock_pag.return_value = []
        # The referenced op declares POST; the leg action_type=PATCH disagrees.
        leg = self._good_leg(action_type="PATCH")
        ve = next(s for s in _build_plan(MagicMock(), _build_config(self._branch_components(leg, method="POST")))["steps"]
                  if s["key"] == "main_process")["validation_error"]
        assert ve["error_code"] == "PROCESS_REF_TYPE_MISMATCH"
        assert ve["field"] == "branch.targets[0].action_type"
        assert ve["details"]["expected_role"] == "POST"
        assert ve["details"]["actual_role"] == "PATCH"

    @patch(_PATCH_TARGET)
    def test_branch_leg_clean_typed_refs_plan_without_error(self, mock_pag):
        mock_pag.return_value = []
        leg = self._good_leg()  # connection->settings, operation->action, action_type matches method
        plan = _build_plan(MagicMock(), _build_config(self._branch_components(leg)))
        process_step = next(s for s in plan["steps"] if s["key"] == "main_process")
        assert process_step.get("validation_error") is None
        assert process_step["planned_action"] == "create"


class TestBuildPlanGeneratedProfileJson:

    @patch(_PATCH_TARGET)
    def test_valid_json_profile_routes_through_builder(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([_json_profile_comp()]))
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step["route"] == "profile_builder_or_xml"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_invalid_json_profile_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _json_profile_comp()
        bad.config["root"]["children"].append(
            {"name": "x", "kind": "simple", "data_type": "blob"}
        )
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_PROFILE_FIELD_TYPE"

    @patch(_PATCH_TARGET)
    def test_wrong_profile_type_errors_with_unsupported_mode(self, mock_pag):
        mock_pag.return_value = []
        bad = _json_profile_comp()
        bad.config["profile_type"] = "database.read"
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_PROFILE_GENERATION_MODE"

    @patch(_PATCH_TARGET)
    def test_secret_shaped_key_redacted(self, mock_pag):
        mock_pag.return_value = []
        bad = _json_profile_comp()
        bad.config["password"] = "sk_live_LEAK"
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = plan["steps"][0]
        assert step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        # The redacted value must appear in the echoed spec (the original
        # value is replaced with [REDACTED]).
        spec_blob = str(plan["integration_spec"])
        assert "sk_live_LEAK" not in spec_blob

    @patch(_PATCH_TARGET)
    def test_raw_xml_bypass_preserved(self, mock_pag):
        mock_pag.return_value = []
        bad = _json_profile_comp()
        bad.config = {
            "component_type": "profile.json",
            "xml": "<bns:Component>...</bns:Component>",
        }
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = plan["steps"][0]
        # Raw XML bypasses structured validation — should plan as create
        # without builder errors.
        assert step["planned_action"] == "create"
        assert "validation_error" not in step


class TestBuildPlanGeneratedProfileXml:

    @patch(_PATCH_TARGET)
    def test_valid_xml_profile_routes_through_builder(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([_xml_profile_comp()]))
        step = plan["steps"][0]
        assert step["planned_action"] == "create"
        assert step["route"] == "profile_builder_or_xml"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_unsupported_xml_feature_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _xml_profile_comp()
        bad.config["root"]["children"][0]["attributes"] = [{"name": "id"}]
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_XML_PROFILE_FEATURE"


class TestBuildPlanTransformMapDirect:

    @patch(_PATCH_TARGET)
    def test_valid_in_spec_map_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _json_profile_comp(),
            _direct_map_comp(),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "create"
        assert map_step["route"] == "map_builder_or_xml"
        assert "validation_error" not in map_step

    @patch(_PATCH_TARGET)
    def test_literal_uuid_profile_id_errors_with_index_unavailable(self, mock_pag):
        mock_pag.return_value = []
        bad = _direct_map_comp(source_ref_key="json_profile", target_ref_key="json_profile")
        # Replace source_profile_id with a literal UUID (no $ref).
        bad.config["source_profile_id"] = "00000000-1111-2222-3333-444444444444"
        plan = _build_plan(MagicMock(), _build_config([_json_profile_comp(), bad]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "MAP_PROFILE_INDEX_UNAVAILABLE"

    @patch(_PATCH_TARGET)
    def test_unknown_target_path_errors_with_map_field_not_found(self, mock_pag):
        mock_pag.return_value = []
        bad = _direct_map_comp(field_mappings=[
            {"source_path": "Root/a", "target_path": "Root/missing"},
        ])
        plan = _build_plan(MagicMock(), _build_config([_json_profile_comp(), bad]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "MAP_FIELD_NOT_FOUND"

    @patch(_PATCH_TARGET)
    def test_duplicate_target_path_errors_with_duplicate_target_mapping(self, mock_pag):
        mock_pag.return_value = []
        bad = _direct_map_comp(field_mappings=[
            {"source_path": "Root/a", "target_path": "Root/a"},
            {"source_path": "Root/list[]/key", "target_path": "Root/a"},
        ])
        plan = _build_plan(MagicMock(), _build_config([_json_profile_comp(), bad]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "DUPLICATE_TARGET_MAPPING"

    @patch(_PATCH_TARGET)
    def test_unsupported_transform_route_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _direct_map_comp()
        bad.config["functions"] = ["foo"]
        plan = _build_plan(MagicMock(), _build_config([_json_profile_comp(), bad]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "UNSUPPORTED_TRANSFORM_ROUTE"

    @patch(_PATCH_TARGET)
    def test_xml_to_json_map_with_in_spec_profiles_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _direct_map_comp(
                source_ref_key="xml_profile",
                target_ref_key="json_profile",
                source_type="profile.xml",
                target_type="profile.json",
                field_mappings=[
                    {"source_path": "rows/row[]/key", "target_path": "Root/list[]/key"},
                ],
            ),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "create"
        assert "validation_error" not in map_step


# ---------------------------------------------------------------------------
# Issue #26 codex r1 P2 fixes — plan/apply mismatch regressions
# ---------------------------------------------------------------------------


class TestBuildPlanIssue26CodexR1Fixes:
    """Plan-time validation gaps that previously let bad configs slip into apply."""

    @patch(_PATCH_TARGET)
    def test_structured_update_runs_validate_config(self, mock_pag):
        """Finding #1: profile.json update path must be validated at plan
        time. Otherwise a bad update (e.g. data_type='blob') plans clean as
        'update' and crashes apply after dependencies have already mutated
        state."""
        mock_pag.return_value = []  # no existing matches → update fails with
        # error_missing_target, but the validate_config error must fire BEFORE
        # that — adjust by mocking an existing component for the update.
        # Simpler: use action='update' WITH component_id supplied so
        # planned_action stays 'update'.
        bad = _json_profile_comp()
        bad.action = "update"
        bad.component_id = "ffffffff-1111-2222-3333-444444444444"
        bad.config["root"]["children"].append(
            {"name": "x", "kind": "simple", "data_type": "blob"}
        )
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_generated_profile_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_PROFILE_FIELD_TYPE"

    @patch(_PATCH_TARGET)
    def test_top_level_named_profile_indexes_correctly_for_map(self, mock_pag):
        """Finding #2: a profile that supplies IntegrationComponentSpec.name
        but no config.component_name must still index successfully so the
        downstream map can validate."""
        mock_pag.return_value = []
        # Profile relies on top-level name; config has no component_name.
        json_profile = _json_profile_comp()
        json_profile.config.pop("component_name", None)
        # Map references that profile — should plan without error because
        # _resolve_map_profile_index injects comp.name into component_name.
        map_comp = _direct_map_comp(source_ref_key="json_profile", target_ref_key="json_profile")
        plan = _build_plan(MagicMock(), _build_config([json_profile, map_comp]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "create"
        assert "validation_error" not in map_step

    @patch(_PATCH_TARGET)
    def test_dollar_ref_to_non_profile_component_fails_index_unavailable(self, mock_pag):
        """Finding #3: $ref:KEY pointing at a non-profile (or unknown)
        component must fail with MAP_PROFILE_INDEX_UNAVAILABLE at plan time,
        not silently apply with unresolved $ref."""
        mock_pag.return_value = []
        # Map's source_profile_id is $ref:db_connection — db_connection is a
        # connector-settings stub (created by _stub_dep_comp), not a profile.
        map_comp = _direct_map_comp(
            source_ref_key="db_connection",
            target_ref_key="json_profile",
            source_type="profile.db",  # claimed type doesn't match actual
        )
        plan = _build_plan(MagicMock(), _build_config([
            _stub_dep_comp("db_connection"),
            _json_profile_comp(),
            map_comp,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "MAP_PROFILE_INDEX_UNAVAILABLE"
        assert map_step["validation_error"]["details"]["side"] == "source"

    @patch(_PATCH_TARGET)
    def test_map_ref_missing_from_depends_on_fails_at_plan(self, mock_pag):
        """Finding #4: a map whose source_profile_id references a profile
        but omits the profile key from depends_on must be rejected at plan
        time. Without this, topological sort may place the map before the
        profile and _resolve_dependency_tokens returns the literal $ref:KEY
        string at apply time."""
        mock_pag.return_value = []
        # Use a separate target profile so we can drop source from depends_on
        # cleanly. Construct map manually since the helper's depends_on is
        # derived from ref keys.
        json_profile = _json_profile_comp()
        target_profile = _json_profile_comp(key="json_tgt", name="Tgt")
        map_comp = IntegrationComponentSpec(
            key="bad_deps_map",
            type="transform.map",
            action="create",
            name="Bad Deps Map",
            depends_on=["json_tgt"],  # MISSING json_profile (the source)
            config={
                "component_type": "transform.map",
                "map_type": "direct",
                "component_name": "Bad Deps Map",
                "source_profile_id": "$ref:json_profile",
                "source_profile_type": "profile.json",
                "target_profile_id": "$ref:json_tgt",
                "target_profile_type": "profile.json",
                "field_mappings": [
                    {"source_path": "Root/a", "target_path": "Root/a"},
                ],
            },
        )
        plan = _build_plan(MagicMock(), _build_config([
            json_profile, target_profile, map_comp,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "bad_deps_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "MAP_PROFILE_REF_REQUIRED"
        assert map_step["validation_error"]["details"]["side"] == "source"
        assert map_step["validation_error"]["details"]["ref_key"] == "json_profile"


# ---------------------------------------------------------------------------
# Issue #40 — transform.map function map plan routing
# ---------------------------------------------------------------------------


def _function_map_comp(
    key="function_map",
    name="Test Function Map",
    source_ref_key="xml_profile",
    target_ref_key="json_profile",
    source_type="profile.xml",
    target_type="profile.json",
    function_mappings=None,
    field_mappings=None,
    map_type="function",
    **config_overrides,
):
    cfg = {
        "component_type": "transform.map",
        "map_type": map_type,
        "component_name": name,
        "source_profile_id": f"$ref:{source_ref_key}",
        "source_profile_type": source_type,
        "target_profile_id": f"$ref:{target_ref_key}",
        "target_profile_type": target_type,
        "function_mappings": function_mappings or [
            {
                "function_type": "lowercase",
                "inputs": ["rows/row[]/key"],
                "target_path": "Root/list[]/key",
                "parameters": {},
            },
        ],
    }
    if field_mappings is not None:
        cfg["field_mappings"] = field_mappings
    cfg.update(config_overrides)
    depends = (
        [source_ref_key, target_ref_key]
        if source_ref_key != target_ref_key
        else [source_ref_key]
    )
    return IntegrationComponentSpec(
        key=key, type="transform.map", action="create", name=name, config=cfg,
        depends_on=depends,
    )


class TestBuildPlanTransformMapFunction:

    @patch(_PATCH_TARGET)
    def test_valid_function_map_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _function_map_comp(),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "function_map")
        assert map_step["planned_action"] == "create"
        assert map_step["route"] == "map_builder_or_xml"
        assert "validation_error" not in map_step

    @patch(_PATCH_TARGET)
    def test_transform_summary_advertises_function_count(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _function_map_comp(
                function_mappings=[
                    {
                        "function_type": "lowercase",
                        "inputs": ["rows/row[]/key"],
                        "target_path": "Root/list[]/key",
                        "parameters": {},
                    },
                    {
                        "function_type": "default_value",
                        "inputs": [],
                        "target_path": "Root/a",
                        "parameters": {"value": "constant"},
                    },
                ],
            ),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "function_map")
        summary = map_step["transform_summary"]
        assert summary["map_type"] == "function"
        assert summary["function_count"] == 2
        assert summary["direct_mapping_count"] == 0
        assert summary["function_types_used"] == ["lowercase", "default_value"]

    @patch(_PATCH_TARGET)
    def test_map_function_alias_routes_through_function_builder(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _function_map_comp(map_type="map_function"),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "function_map")
        assert map_step["planned_action"] == "create"
        assert map_step["transform_summary"]["map_type"] == "map_function"

    @patch(_PATCH_TARGET)
    def test_literal_uuid_source_errors_with_index_unavailable(self, mock_pag):
        mock_pag.return_value = []
        bad = _function_map_comp()
        bad.config["source_profile_id"] = "00000000-1111-2222-3333-444444444444"
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "function_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "MAP_PROFILE_INDEX_UNAVAILABLE"

    @patch(_PATCH_TARGET)
    def test_unknown_function_type_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _function_map_comp(function_mappings=[
            {
                "function_type": "fake_function",
                "inputs": ["rows/row[]/key"],
                "target_path": "Root/list[]/key",
                "parameters": {},
            },
        ])
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "function_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "UNSUPPORTED_MAP_FUNCTION_TYPE"

    @patch(_PATCH_TARGET)
    def test_duplicate_target_across_function_and_field_mappings(self, mock_pag):
        mock_pag.return_value = []
        bad = _function_map_comp(
            function_mappings=[
                {
                    "function_type": "lowercase",
                    "inputs": ["rows/row[]/key"],
                    "target_path": "Root/a",
                    "parameters": {},
                },
            ],
            field_mappings=[
                {"source_path": "rows/row[]/key", "target_path": "Root/a"},
            ],
        )
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "function_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "DUPLICATE_TARGET_MAPPING"

    @patch(_PATCH_TARGET)
    def test_math_unsupported_operation_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _function_map_comp(function_mappings=[
            {
                "function_type": "math",
                "inputs": ["rows/row[]/key", "rows/row[]/key"],
                "target_path": "Root/a",
                "parameters": {"operation": "modulo"},
            },
        ])
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "function_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "UNSUPPORTED_MATH_OPERATION"

    @patch(_PATCH_TARGET)
    def test_direct_map_summary_includes_zero_function_count(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _json_profile_comp(),
            _direct_map_comp(),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "json_to_json_map")
        summary = map_step["transform_summary"]
        assert summary["map_type"] == "direct"
        assert summary["function_count"] == 0
        assert summary["direct_mapping_count"] >= 1
        assert summary["function_types_used"] == []


# ---------------------------------------------------------------------------
# Issue #41 — script.mapping component + transform.map script-route routing
# ---------------------------------------------------------------------------


def _script_mapping_comp(
    key="enrich_row_script",
    name="Test Script Mapping",
    **config_overrides,
):
    cfg = {
        "component_type": "script.mapping",
        "component_name": name,
        "language": "groovy2",
        "script_body": "outputValue = inputValue.toUpperCase()",
        "inputs": [{"name": "inputValue", "data_type": "character"}],
        "outputs": [{"name": "outputValue"}],
    }
    cfg.update(config_overrides)
    return IntegrationComponentSpec(
        key=key, type="script.mapping", action="create", name=name, config=cfg,
    )


def _script_map_comp(
    key="script_map",
    name="Test Script Map",
    source_ref_key="xml_profile",
    target_ref_key="json_profile",
    source_type="profile.xml",
    target_type="profile.json",
    script_mappings=None,
    field_mappings=None,
    map_type="script",
    script_ref_keys=("enrich_row_script",),
    **config_overrides,
):
    cfg = {
        "component_type": "transform.map",
        "map_type": map_type,
        "component_name": name,
        "source_profile_id": f"$ref:{source_ref_key}",
        "source_profile_type": source_type,
        "target_profile_id": f"$ref:{target_ref_key}",
        "target_profile_type": target_type,
        "script_mappings": script_mappings or [
            {
                "script_component_id": f"$ref:{script_ref_keys[0]}",
                "script_slot": "enrich_row",
                "inputs": [
                    {"source_path": "rows/row[]/key", "input_name": "inputValue"},
                ],
                "outputs": [
                    {"output_name": "outputValue", "target_path": "Root/list[]/key"},
                ],
            },
        ],
    }
    if field_mappings is not None:
        cfg["field_mappings"] = field_mappings
    cfg.update(config_overrides)
    depends = [source_ref_key, target_ref_key, *script_ref_keys]
    # Deduplicate while preserving order.
    seen = set()
    depends_dedup = []
    for d in depends:
        if d not in seen:
            seen.add(d)
            depends_dedup.append(d)
    return IntegrationComponentSpec(
        key=key, type="transform.map", action="create", name=name, config=cfg,
        depends_on=depends_dedup,
    )


class TestBuildPlanScriptMappingComponent:

    @patch(_PATCH_TARGET)
    def test_valid_script_mapping_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([_script_mapping_comp()]))
        step = next(s for s in plan["steps"] if s["key"] == "enrich_row_script")
        assert step["planned_action"] == "create"
        assert "validation_error" not in step

    @patch(_PATCH_TARGET)
    def test_unsupported_language_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _script_mapping_comp()
        bad.config["language"] = "python"
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = next(s for s in plan["steps"] if s["key"] == "enrich_row_script")
        assert step["planned_action"] == "error_generated_profile_validation"
        assert (
            step["validation_error"]["error_code"]
            == "SCRIPT_MAPPING_LANGUAGE_UNSUPPORTED"
        )

    @patch(_PATCH_TARGET)
    def test_missing_script_body_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _script_mapping_comp()
        del bad.config["script_body"]
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = next(s for s in plan["steps"] if s["key"] == "enrich_row_script")
        assert step["planned_action"] == "error_generated_profile_validation"
        assert (
            step["validation_error"]["error_code"]
            == "SCRIPT_MAPPING_BODY_REQUIRED"
        )

    @patch(_PATCH_TARGET)
    def test_secret_shaped_key_redacted(self, mock_pag):
        mock_pag.return_value = []
        bad = _script_mapping_comp()
        bad.config["password"] = "leaked"
        plan = _build_plan(MagicMock(), _build_config([bad]))
        step = next(s for s in plan["steps"] if s["key"] == "enrich_row_script")
        assert step["planned_action"] == "error_generated_profile_validation"
        assert (
            step["validation_error"]["error_code"] == "PLAINTEXT_SECRET_REJECTED"
        )
        # Original config has the secret redacted in the plan echo.
        emitted = plan["integration_spec"]["components"][0]["config"]
        assert emitted["password"] == "[REDACTED]"


class TestBuildPlanTransformMapScript:

    @patch(_PATCH_TARGET)
    def test_valid_script_map_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            _script_map_comp(),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "create"
        assert "validation_error" not in map_step

    @patch(_PATCH_TARGET)
    def test_map_script_alias_routes_through_script_builder(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            _script_map_comp(map_type="map_script"),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "create"
        assert map_step["transform_summary"]["map_type"] == "map_script"

    @patch(_PATCH_TARGET)
    def test_transform_summary_advertises_script_count_slots_languages(self, mock_pag):
        mock_pag.return_value = []
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            _script_mapping_comp(key="other_script", name="Other Script"),
            _script_map_comp(
                script_mappings=[
                    {
                        "script_component_id": "$ref:enrich_row_script",
                        "script_slot": "enrich_row",
                        "language": "groovy2",
                        "inputs": [
                            {"source_path": "rows/row[]/key", "input_name": "inputValue"},
                        ],
                        "outputs": [
                            {"output_name": "outputValue", "target_path": "Root/list[]/key"},
                        ],
                    },
                    {
                        "script_component_id": "$ref:other_script",
                        "script_slot": "other_slot",
                        "language": "javascript",
                        "inputs": [
                            {"source_path": "rows/row[]/key", "input_name": "inputValue"},
                        ],
                        "outputs": [
                            {"output_name": "outputValue", "target_path": "Root/a"},
                        ],
                    },
                ],
                script_ref_keys=("enrich_row_script", "other_script"),
            ),
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        summary = map_step["transform_summary"]
        assert summary["map_type"] == "script"
        assert summary["script_count"] == 2
        assert summary["script_slots_used"] == ["enrich_row", "other_slot"]
        assert summary["script_languages_used"] == ["groovy2", "javascript"]

    @patch(_PATCH_TARGET)
    def test_script_ref_without_depends_on_is_safe_via_wrapper_synthesis(self, mock_pag):
        # After the Issue #41 r3 wrapper synthesis pass, omitting the
        # script.mapping key from the calling map's depends_on is safe:
        # plan-time synthesis injects an auto-synthesized
        # transform.function wrapper, adds the wrapper key to the map's
        # depends_on, and the wrapper itself depends on the script.mapping.
        # Topological order stays correct via the wrapper hop.
        mock_pag.return_value = []
        sloppy = _script_map_comp()
        # Drop the script ref from the map's depends_on — synthesis
        # should not require the caller to list it explicitly.
        sloppy.depends_on = [d for d in sloppy.depends_on if d != "enrich_row_script"]
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            sloppy,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "create"
        assert "validation_error" not in map_step
        # The auto-synthesized wrapper appears in the spec with the
        # script.mapping as its sole dependency.
        wrapper_key = "__auto_wrapper_enrich_row_script__"
        wrapper = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == wrapper_key
        )
        assert wrapper["type"] == "transform.function"
        assert "enrich_row_script" in wrapper["depends_on"]
        # The map's depends_on now includes the wrapper (auto-added by
        # synthesis), and execution_order runs script → wrapper → map.
        the_map = next(
            c for c in plan["integration_spec"]["components"]
            if c["key"] == "script_map"
        )
        assert wrapper_key in the_map["depends_on"]
        order = plan["execution_order"]
        assert order.index("enrich_row_script") < order.index(wrapper_key)
        assert order.index(wrapper_key) < order.index("script_map")

    @patch(_PATCH_TARGET)
    def test_literal_uuid_source_errors_with_index_unavailable(self, mock_pag):
        mock_pag.return_value = []
        bad = _script_map_comp()
        bad.config["source_profile_id"] = "00000000-1111-2222-3333-444444444444"
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert (
            map_step["validation_error"]["error_code"]
            == "MAP_PROFILE_INDEX_UNAVAILABLE"
        )

    @patch(_PATCH_TARGET)
    def test_unresolved_source_path_errors_with_map_field_not_found(self, mock_pag):
        mock_pag.return_value = []
        # Port names match _script_mapping_comp's declared inputValue /
        # outputValue so the Codex r5 port-shape check passes and the
        # MAP_FIELD_NOT_FOUND failure for the bad source_path surfaces.
        bad = _script_map_comp(
            script_mappings=[
                {
                    "script_component_id": "$ref:enrich_row_script",
                    "script_slot": "enrich_row",
                    "inputs": [
                        {"source_path": "rows/row[]/missing", "input_name": "inputValue"},
                    ],
                    "outputs": [
                        {"output_name": "outputValue", "target_path": "Root/list[]/key"},
                    ],
                },
            ],
        )
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert map_step["validation_error"]["error_code"] == "MAP_FIELD_NOT_FOUND"

    @patch(_PATCH_TARGET)
    def test_unsupported_map_type_errors_at_plan(self, mock_pag):
        mock_pag.return_value = []
        bad = _script_map_comp(map_type="bogus")
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert (
            map_step["validation_error"]["error_code"]
            == "UNSUPPORTED_TRANSFORM_ROUTE"
        )

    @patch(_PATCH_TARGET)
    def test_mixed_direct_and_script_route_plans_clean(self, mock_pag):
        mock_pag.return_value = []
        comp = _script_map_comp(
            field_mappings=[
                {"source_path": "rows/row[]/key", "target_path": "Root/a"},
            ],
        )
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            comp,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "create"
        summary = map_step["transform_summary"]
        assert summary["direct_mapping_count"] == 1
        assert summary["script_count"] == 1

    @patch(_PATCH_TARGET)
    def test_duplicate_target_across_field_and_script_outputs_rejected(self, mock_pag):
        mock_pag.return_value = []
        # field_mappings binds Root/list[]/key; the default script_map binds
        # the same target via script_mappings[0].outputs[0].target_path.
        bad = _script_map_comp(
            field_mappings=[
                {"source_path": "rows/row[]/key", "target_path": "Root/list[]/key"},
            ],
        )
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert (
            map_step["validation_error"]["error_code"]
            == "DUPLICATE_TARGET_MAPPING"
        )

    @patch(_PATCH_TARGET)
    def test_function_mappings_rejected_on_script_route(self, mock_pag):
        mock_pag.return_value = []
        bad = _script_map_comp()
        bad.config["function_mappings"] = [
            {"function_type": "uppercase", "inputs": [], "target_path": "x"},
        ]
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        assert (
            map_step["validation_error"]["error_code"]
            == "UNSUPPORTED_TRANSFORM_ROUTE"
        )
        assert map_step["validation_error"]["field"] == "function_mappings"

    @patch(_PATCH_TARGET)
    def test_script_ref_pointing_at_non_script_component_rejected(self, mock_pag):
        # Codex r1 P2 finding #4: $ref:KEY for script_component_id whose
        # target isn't a script.mapping (here: a profile.xml) must fail at
        # plan time. Without this guard, apply would resolve the $ref to a
        # profile UUID and emit a userdefined FunctionStep whose id points
        # at the wrong component type.
        mock_pag.return_value = []
        bad = _script_map_comp(
            script_mappings=[
                {
                    "script_component_id": "$ref:xml_profile",
                    "script_slot": "wrong",
                    "inputs": [
                        {"source_path": "rows/row[]/key", "input_name": "in"},
                    ],
                    "outputs": [
                        {"output_name": "out", "target_path": "Root/list[]/key"},
                    ],
                },
            ],
            script_ref_keys=("xml_profile",),
        )
        plan = _build_plan(MagicMock(), _build_config([
            _xml_profile_comp(),
            _json_profile_comp(),
            _script_mapping_comp(),
            bad,
        ]))
        map_step = next(s for s in plan["steps"] if s["key"] == "script_map")
        assert map_step["planned_action"] == "error_generated_profile_validation"
        validation_error = map_step["validation_error"]
        assert validation_error["error_code"] == "SCRIPT_MAPPING_REF_REQUIRED"
        # The field points at the misrouted script_component_id, not
        # depends_on (which is the topo-sort guard, a separate case).
        assert "script_component_id" in validation_error["field"]
        assert validation_error["details"]["target_component_type"] == "profile.xml"


class TestScriptMappingMetadataRegistrationAndDefaults:
    """Codex r1 P2 findings #2 and #3:
    - script.mapping must participate in metadata lookup (conflict_policy
      reuse/fail + update-by-name).
    - Apply-time must inject comp.name into payload['component_name'] when
      the spec carries only a top-level name, mirroring plan-time."""

    def test_script_mapping_in_metadata_type_map(self):
        from boomi_mcp.categories.integration_builder import _METADATA_TYPE_MAP
        assert _METADATA_TYPE_MAP.get("script.mapping") == "script.mapping"

    def test_apply_time_name_default_covers_script_mapping(self):
        # Walking _execute_component is invasive; instead verify the
        # source-level dispatch list literally contains script.mapping.
        # Documents the fix to Codex P2 #3 — without script.mapping in
        # the name-defaulting elif, a clean plan would fail at apply with
        # 'component_name is required'.
        import inspect
        from boomi_mcp.categories import integration_builder as ib
        source = inspect.getsource(ib._execute_component)
        # The block: ``elif comp.type in (..., "script.mapping",):
        # payload.setdefault("component_name", comp.name)``
        assert '"script.mapping",' in source
        assert 'payload.setdefault("component_name", comp.name)' in source


# ============================================================================
# Issue #45 — build_integration plan/apply preservation wiring
# ============================================================================


class TestBuildPlanUpdatePreservationMetadata:
    """Plan output exposes update_mode / preserves_unknown_xml /
    owned_paths / preserved_paths on every structured-route update step."""

    @patch(_PATCH_TARGET)
    def test_structured_builder_update_step_carries_read_merge_write(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-existing", "Example SQL Server", comp_type="connector-settings"),
        ]
        comp = _db_comp(action="update", component_id="id-existing")
        result = _build_plan(MagicMock(), _build_config([comp]))
        assert result["_success"] is True
        step = result["steps"][0]
        assert step["planned_action"] == "update"
        assert step["update_mode"] == "read_merge_write"
        assert step["preserves_unknown_xml"] is True
        assert "bns:object/DatabaseConnectionSettings" in step["owned_paths"]
        assert "bns:encryptedValues" in step["preserved_paths"]
        assert "bns:processOverrides" in step["preserved_paths"]

    @patch(_PATCH_TARGET)
    def test_raw_xml_update_step_carries_full_xml_replace(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-existing", "Raw XML Connector", comp_type="connector-settings"),
        ]
        comp = IntegrationComponentSpec(
            key="raw-conn",
            type="connector-settings",
            action="update",
            name="Raw XML Connector",
            component_id="id-existing",
            config={"xml": "<bns:Component xmlns:bns='http://api.platform.boomi.com/' "
                            "type='connector-settings' subType='database' name='Raw XML Connector'/>"},
        )
        result = _build_plan(MagicMock(), _build_config([comp]))
        assert result["_success"] is True
        step = result["steps"][0]
        assert step["planned_action"] == "update"
        assert step["update_mode"] == "full_xml_replace"
        assert step["preserves_unknown_xml"] is False
        assert "owned_paths" not in step

    @patch(_PATCH_TARGET)
    def test_create_step_omits_update_mode(self, mock_pag):
        mock_pag.return_value = []  # no existing → fresh create
        comp = _db_comp(action="create")
        result = _build_plan(MagicMock(), _build_config([comp]))
        step = result["steps"][0]
        assert step["planned_action"] == "create"
        assert "update_mode" not in step
        assert "preserves_unknown_xml" not in step
        assert "owned_paths" not in step

    @patch(_PATCH_TARGET)
    def test_reuse_step_omits_update_mode(self, mock_pag):
        mock_pag.return_value = [
            _meta("id-existing", "Example SQL Server", comp_type="connector-settings"),
        ]
        # conflict_policy=reuse + action=create on an existing match → reuse
        comp = _db_comp(action="create")
        result = _build_plan(
            MagicMock(),
            _build_config([comp], conflict_policy="reuse"),
        )
        step = result["steps"][0]
        assert step["planned_action"] == "reuse"
        assert "update_mode" not in step


class TestApplyStructuredUpdate:
    """The apply-time helper fetches current XML, merges, and pushes via
    update_component_raw — never via update_component with raw built XML."""

    def test_apply_structured_update_fetches_current_xml_and_pushes_merged(self):
        from src.boomi_mcp.categories.integration_builder import (
            _apply_structured_update,
        )
        from src.boomi_mcp.categories.components.builders.connector_builder import (
            DatabaseConnectorBuilder,
        )

        boomi_client = MagicMock()
        # Current live XML carries an unknown root attr (must survive) and
        # an encryptedValues entry with isSet=true (must survive).
        current_xml = (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
            'type="connector-settings" subType="database" name="old" '
            'futureFlag="opaque">'
            '<bns:encryptedValues>'
            '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="true"/>'
            '</bns:encryptedValues>'
            '<bns:description>desc</bns:description>'
            '<bns:object>'
            '<DatabaseConnectionSettings xmlns="" dbname="olddb" host="old.example.com" '
            'port="3306" username="x"/>'
            '</bns:object>'
            '</bns:Component>'
        )
        # Codex r7 P2: built_xml must include the password xpath
        # marker the real builder emits, so the owned_encrypted_paths
        # prune logic correctly preserves the live isSet=true secret.
        built_xml = (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
            'type="connector-settings" subType="database" name="renamed">'
            '<bns:encryptedValues>'
            '<bns:encryptedValue path="//DatabaseConnectionSettings/@password" isSet="false"/>'
            '</bns:encryptedValues>'
            '<bns:description>new</bns:description>'
            '<bns:object>'
            '<DatabaseConnectionSettings xmlns="" dbname="newdb" host="db.example.com" '
            'port="5432" username="y"/>'
            '</bns:object>'
            '</bns:Component>'
        )
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            return_value={"xml": current_xml, "type": "connector-settings"},
        ):
            result = _apply_structured_update(
                boomi_client,
                "test_profile",
                "id-existing",
                _db_comp(action="update", component_id="id-existing"),
                built_xml,
                DatabaseConnectorBuilder.PRESERVATION_POLICY,
            )
        assert result["_success"] is True
        # update_component_raw was called with merged XML
        boomi_client.component.update_component_raw.assert_called_once()
        call_args = boomi_client.component.update_component_raw.call_args
        merged_xml = call_args[0][1]
        # Unknown attr survived
        assert 'futureFlag="opaque"' in merged_xml
        # Owned attrs replaced
        assert 'name="renamed"' in merged_xml
        # Owned subtree replaced
        assert 'dbname="newdb"' in merged_xml
        assert 'dbname="olddb"' not in merged_xml
        # Existing encryptedValue (isSet=true) preserved
        assert 'isSet="true"' in merged_xml

    def test_apply_structured_update_missing_policy_short_circuits_before_fetch(self):
        from src.boomi_mcp.categories.integration_builder import (
            _apply_structured_update,
        )

        boomi_client = MagicMock()
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml"
        ) as mock_fetch:
            result = _apply_structured_update(
                boomi_client,
                "test_profile",
                "id-existing",
                _db_comp(action="update", component_id="id-existing"),
                "<bns:Component/>",
                None,
            )
        assert result["_success"] is False
        assert result["error_code"] == "UPDATE_PRESERVATION_POLICY_UNSUPPORTED"
        mock_fetch.assert_not_called()
        boomi_client.component.update_component_raw.assert_not_called()

    def test_apply_structured_update_fetch_failure_surfaced_structured(self):
        from src.boomi_mcp.categories.integration_builder import (
            _apply_structured_update,
        )
        from src.boomi_mcp.categories.components.builders.connector_builder import (
            DatabaseConnectorBuilder,
        )

        boomi_client = MagicMock()
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            side_effect=Exception("GET failed: 404"),
        ):
            result = _apply_structured_update(
                boomi_client,
                "test_profile",
                "id-missing",
                _db_comp(action="update", component_id="id-missing"),
                "<bns:Component/>",
                DatabaseConnectorBuilder.PRESERVATION_POLICY,
            )
        assert result["_success"] is False
        assert result["error_code"] == "UPDATE_PRESERVATION_FETCH_FAILED"
        boomi_client.component.update_component_raw.assert_not_called()

    def test_apply_structured_update_merge_failure_short_circuits_push(self):
        from src.boomi_mcp.categories.integration_builder import (
            _apply_structured_update,
        )
        from src.boomi_mcp.categories.components.builders.connector_builder import (
            DatabaseConnectorBuilder,
        )

        boomi_client = MagicMock()
        # Type mismatch — current is connector-action, policy expects connector-settings
        bad_current_xml = (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
            'type="connector-action" subType="database" name="x"/>'
        )
        built_xml = (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
            'type="connector-settings" subType="database" name="x">'
            '<bns:object>'
            '<DatabaseConnectionSettings xmlns=""/>'
            '</bns:object>'
            '</bns:Component>'
        )
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            return_value={"xml": bad_current_xml, "type": "connector-action"},
        ):
            result = _apply_structured_update(
                boomi_client,
                "test_profile",
                "id-existing",
                _db_comp(action="update", component_id="id-existing"),
                built_xml,
                DatabaseConnectorBuilder.PRESERVATION_POLICY,
            )
        assert result["_success"] is False
        assert result["error_code"] == "UPDATE_PRESERVATION_TYPE_MISMATCH"
        boomi_client.component.update_component_raw.assert_not_called()


# ============================================================================
# Codex review r2 — plan-time validation of structured updates
# ============================================================================


class TestPlanTimeValidationOfUpdates:
    """Codex r2 P2: builder updates must validate at plan time so apply
    failures don't leave the system half-mutated. Updates that invoke
    the structured builder run the full validator; metadata-only updates
    bypass the builder so they're not validated against builder rules."""

    @patch(_PATCH_TARGET)
    def test_profile_db_update_with_missing_query_fails_plan(self, mock_pag):
        """Codex r2 P2: profile.db update now invokes the structured
        builder, so missing query must be caught at plan time."""
        comp = IntegrationComponentSpec(
            key="db_profile",
            type="profile.db",
            action="update",
            name="Example Read Profile",
            component_id="explicit-profile-id",
            config={
                "component_type": "profile.db",
                "profile_type": "database.read",
                "component_name": "Example Read Profile",
                # query intentionally missing
                "output_fields": [{"name": "one"}],
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "MISSING_DB_QUERY"

    @patch(_PATCH_TARGET)
    def test_profile_db_update_with_unsupported_profile_type_fails_plan(self, mock_pag):
        # Include body fields (query, output_fields) so this is treated
        # as a structured-builder update — metadata-only updates with
        # an unsupported profile_type would just route through smart-merge
        # and ignore profile_type entirely (Codex r3 P2 follow-up).
        comp = IntegrationComponentSpec(
            key="db_profile",
            type="profile.db",
            action="update",
            name="Example Read Profile",
            component_id="explicit-profile-id",
            config={
                "component_type": "profile.db",
                "profile_type": "database.write",  # unsupported in M2
                "component_name": "Example Read Profile",
                "query": "SELECT 1 AS one",
                "output_fields": [{"name": "one"}],
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert (
            step["validation_error"]["error_code"]
            == "UNSUPPORTED_DB_PROFILE_MODE"
        )

    @patch(_PATCH_TARGET)
    def test_profile_db_metadata_only_update_bypasses_builder(self, mock_pag):
        """Codex r3 P2: profile.db updates with ONLY metadata fields
        bypass the structured builder and route through smart-merge,
        matching the pre-#45 behaviour for renames/description edits."""
        comp = IntegrationComponentSpec(
            key="db_profile",
            type="profile.db",
            action="update",
            name="Example Read Profile",
            component_id="explicit-profile-id",
            config={
                "component_type": "profile.db",
                "component_name": "Renamed Profile",
                "description": "rename via build_integration",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert "validation_error" not in step
        assert step["update_mode"] == "metadata_smart_merge"
        assert step["preserves_unknown_xml"] is True

    @patch(_PATCH_TARGET)
    def test_rest_op_update_with_missing_path_fails_plan(self, mock_pag):
        """REST connector-action update with body fields (method, but no
        path) invokes the builder at apply, so missing path is caught
        at plan time."""
        # Stub the upstream REST connection so the operation's
        # connection_ref_key resolves to an in-spec dependency.
        rest_conn_stub = IntegrationComponentSpec(
            key="rest_conn",
            type="connector-settings",
            action="update",
            name="Stub Conn",
            component_id="explicit-conn-id",
            config={"connector_type": "rest", "name": "Stub Conn"},
        )
        comp = IntegrationComponentSpec(
            key="rest_op",
            type="connector-action",
            action="update",
            name="Stub Op",
            component_id="explicit-op-id",
            config={
                "connector_type": "rest",
                "operation_mode": "execute",
                "method": "POST",
                "connection_ref_key": "rest_conn",
                "component_name": "Stub Op",
                # path intentionally missing
            },
            depends_on=["rest_conn"],
        )
        plan = _build_plan(MagicMock(), _build_config([rest_conn_stub, comp]))
        op_step = next(s for s in plan["steps"] if s["key"] == "rest_op")
        # Should be error_rest_validation, not a clean update step
        assert op_step["planned_action"] == "error_rest_validation"

    @patch(_PATCH_TARGET)
    def test_metadata_only_connector_update_bypasses_builder_validation(self, mock_pag):
        """Mirror of the positive case from TestBuildPlanDatabaseConnectorPreflight:
        when only metadata fields are present, the builder is not invoked
        and validation does not run."""
        comp = IntegrationComponentSpec(
            key="rest_conn",
            type="connector-settings",
            action="update",
            name="Example REST Conn",
            component_id="explicit-rest-id",
            config={
                "connector_type": "rest",
                "name": "Renamed REST",
                # No base_url, auth, oauth2, etc. → metadata-only
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert "validation_error" not in step
        assert step["update_mode"] == "metadata_smart_merge"


class TestCodexR4Followups:
    """Codex r4 P2 regressions."""

    @patch(_PATCH_TARGET)
    def test_profile_db_metadata_only_rename_via_component_name_routes_to_smart_merge(self, mock_pag):
        """Codex r4 P2 finding #1: profile.db metadata-only updates that
        carry the schema-template field `component_name` (not `name`)
        must route through smart-merge AND actually rename — pre-fix
        update_component ignored component_name."""
        comp = IntegrationComponentSpec(
            key="db_profile",
            type="profile.db",
            action="update",
            name="Existing Profile",
            component_id="explicit-profile-id",
            config={
                "component_type": "profile.db",
                "component_name": "Renamed Profile",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["update_mode"] == "metadata_smart_merge"

    def test_update_component_consumes_component_name_alias(self):
        """update_component now treats component_name as an alias for name."""
        from src.boomi_mcp.categories.components.manage_component import (
            update_component,
        )
        import xml.etree.ElementTree as ET

        boomi_client = MagicMock()
        current_xml = (
            '<bns:Component xmlns:bns="http://api.platform.boomi.com/" '
            'type="profile.db" name="old" componentId="cid"/>'
        )
        with patch(
            "src.boomi_mcp.categories.components.manage_component.component_get_xml",
            return_value={"xml": current_xml, "name": "old"},
        ):
            result = update_component(
                boomi_client,
                "dev",
                "cid",
                {"component_name": "new-name-via-alias"},
            )
        assert result["_success"] is True
        boomi_client.component.update_component_raw.assert_called_once()
        sent_xml = boomi_client.component.update_component_raw.call_args[0][1]
        assert 'name="new-name-via-alias"' in sent_xml

    @patch(_PATCH_TARGET)
    def test_non_string_connector_type_does_not_crash_plan(self, mock_pag):
        """Codex r4 P2 finding #2: a non-string connector_type plus body
        fields used to crash _resolve_preservation_policy with
        AttributeError during planning. Now it should plan with no
        policy resolved and surface a normal validation envelope."""
        comp = IntegrationComponentSpec(
            key="bad_rest",
            type="connector-settings",
            action="update",
            name="Bad Connector",
            component_id="explicit-id",
            config={
                "connector_type": 123,  # int, not str — would crash .lower()
                "base_url": "https://example.com",
                "auth": "NONE",
            },
        )
        # Should not raise; plan completes (validation_error envelope is fine).
        plan = _build_plan(MagicMock(), _build_config([comp]))
        assert plan["_success"] is True
        step = plan["steps"][0]
        # update_mode should NOT be read_merge_write (no policy resolved).
        assert step.get("update_mode") != "read_merge_write"


class TestCodexR5Followups:
    """Codex r5 P2: plan-output update_mode classification accuracy."""

    @patch(_PATCH_TARGET)
    def test_generic_component_metadata_only_update_reports_smart_merge(self, mock_pag):
        """Codex r5 P2: a generic `type='component'` update with only
        metadata fields routes through update_component smart-merge at
        apply (preserves body XML), so the plan must report
        update_mode='metadata_smart_merge' not 'full_xml_replace'."""
        comp = IntegrationComponentSpec(
            key="generic",
            type="component",
            action="update",
            name="Some Existing Component",
            component_id="explicit-id",
            config={
                "name": "Renamed Component",
                "description": "rename",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert step["update_mode"] == "metadata_smart_merge"
        assert step["preserves_unknown_xml"] is True

    @patch(_PATCH_TARGET)
    def test_trading_partner_update_reports_smart_merge(self, mock_pag):
        """trading_partner updates use update_trading_partner's JSON
        partial-update path — preserves unknown fields in the live
        component, equivalent to a smart-merge."""
        comp = IntegrationComponentSpec(
            key="partner",
            type="trading_partner",
            action="update",
            name="Existing Partner",
            component_id="explicit-tp-id",
            config={"component_name": "Renamed Partner"},
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["update_mode"] == "metadata_smart_merge"
        assert step["preserves_unknown_xml"] is True

    @patch(_PATCH_TARGET)
    def test_process_update_without_process_kind_errors_process_kind_required(self, mock_pag):
        """Process updates without process_kind are rejected at plan time;
        legacy freeform process JSON authoring has been removed."""
        comp = IntegrationComponentSpec(
            key="untyped_proc",
            type="process",
            action="update",
            name="Untyped Process",
            component_id="explicit-proc-id",
            config={"name": "Untyped Process"},  # no process_kind → rejected
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_KIND_REQUIRED"


class TestCodexR9Followups:
    """Codex r9 P2 regressions."""

    @patch(_PATCH_TARGET)
    def test_non_string_profile_type_does_not_crash_plan(self, mock_pag):
        """Codex r9 P2: profile.db update with body fields and a
        non-string profile_type (e.g. JSON int) used to crash
        ``_build_plan`` with AttributeError on ``.lower()``. Now it
        returns a clean plan with a structured validation error."""
        comp = IntegrationComponentSpec(
            key="bad_profile",
            type="profile.db",
            action="update",
            name="Existing Profile",
            component_id="explicit-id",
            config={
                "component_type": "profile.db",
                "profile_type": 123,  # int, not str — would crash .lower()
                "query": "SELECT 1",
                "output_fields": [{"name": "x"}],
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        assert plan["_success"] is True
        step = plan["steps"][0]
        # Should be a clean error envelope, not a crash.
        assert step["planned_action"] in (
            "error_database_validation",
            "error_generated_profile_validation",
        )
        assert "validation_error" in step


class TestCodexR11Followups:
    """Codex r11 P2: connector body update with unsupported connector_type
    must surface a structured error, not silently smart-merge metadata."""

    @patch(_PATCH_TARGET)
    def test_unsupported_connector_type_with_body_fields_raises_structured_error(self, mock_pag):
        # connector_type='http' is not in CONNECTOR_BUILDERS (M2 supports
        # database + rest only). Body fields (base_url, auth) signal
        # structured-builder intent; falling through to update_connector
        # would silently smart-merge metadata only.
        from src.boomi_mcp.categories.integration_builder import _execute_component
        boomi_client = MagicMock()
        comp = IntegrationComponentSpec(
            key="http_conn", type="connector-settings", action="update",
            name="HTTP", component_id="explicit-id",
            config={
                "connector_type": "http",  # unsupported in M2
                "base_url": "https://example.com",
                "auth": "NONE",
            },
        )
        result = _execute_component(
            boomi_client, "dev", comp, comp.config, target_id="explicit-id",
        )
        assert result["_success"] is False
        assert result["error_code"] == "UPDATE_PRESERVATION_POLICY_UNSUPPORTED"
        assert "structured builder" in result["error"].lower()


class TestCodexR12Followups:
    """Codex r12 P2: plan-time guard against unsupported-connector body updates."""

    @patch(_PATCH_TARGET)
    def test_unsupported_connector_body_update_surfaces_at_plan(self, mock_pag):
        """connector_type='http' with body fields used to be reported as
        update_mode='metadata_smart_merge' at plan time, but apply
        rejected with UPDATE_PRESERVATION_POLICY_UNSUPPORTED. The plan
        must now surface the same error so a multi-step apply doesn't
        mutate earlier components before failing here."""
        comp = IntegrationComponentSpec(
            key="http_conn",
            type="connector-settings",
            action="update",
            name="HTTP",
            component_id="explicit-id",
            config={
                "connector_type": "http",  # unsupported in M2
                "base_url": "https://example.com",
                "auth": "NONE",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        assert plan["_success"] is True
        step = plan["steps"][0]
        assert step["planned_action"] != "update"
        assert "validation_error" in step
        assert (
            step["validation_error"]["error_code"]
            == "UPDATE_PRESERVATION_POLICY_UNSUPPORTED"
        )
        # update_mode should NOT have been set to metadata_smart_merge:
        # the step is now an error step, not a planned update.
        assert step.get("update_mode") != "metadata_smart_merge"

    @patch(_PATCH_TARGET)
    def test_supported_database_connector_body_update_still_plans_clean(self, mock_pag):
        """The r12 guard must NOT fire for supported builders."""
        comp = _db_comp(action="update")
        comp.component_id = "explicit-id"
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert step.get("update_mode") == "read_merge_write"


class TestCodexR13Followups:
    """Codex r13 P2: error_unsupported_structured_update must trigger fail-fast in _apply_plan."""

    @patch(_PATCH_TARGET)
    def test_unsupported_structured_update_fail_fast_blocks_apply(self, mock_pag):
        """Codex r13 P2: when a multi-step plan contains an
        error_unsupported_structured_update step, _apply_plan must
        refuse to execute ANY component (fail-fast). Pre-fix it would
        execute earlier components before hitting the unsupported
        step's runtime check."""
        # An unsupported connector update should land in the fail-fast set.
        bad = IntegrationComponentSpec(
            key="http_conn",
            type="connector-settings",
            action="update",
            name="HTTP",
            component_id="explicit-id",
            config={
                "connector_type": "http",  # unsupported in M2
                "base_url": "https://example.com",
                "auth": "NONE",
            },
        )
        config = _build_config([bad])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        # Apply must refuse to execute and return the unresolvable_steps
        # diagnostic envelope instead of partial success.
        assert result["_success"] is False
        assert "unresolvable_steps" in result
        assert any(
            s["key"] == "http_conn"
            and s["planned_action"] == "error_unsupported_structured_update"
            for s in result["unresolvable_steps"]
        )


class TestProcessKindRequiredEnforcement:
    """Legacy freeform process authoring removed: untyped process create/update
    is rejected at apply time too (defensive guard), and raw process XML now
    lives only on the manage_component (type='component') escape hatch."""

    def test_execute_component_rejects_untyped_process_create(self):
        from src.boomi_mcp.categories.integration_builder import _execute_component
        boomi_client = MagicMock()
        comp = IntegrationComponentSpec(
            key="untyped_proc",
            type="process",
            action="create",
            name="Untyped Process",
            config={"name": "Untyped Process"},  # no process_kind
        )
        result = _execute_component(boomi_client, "dev", comp, comp.config)
        assert result["_success"] is False
        assert result["error_code"] == "PROCESS_KIND_REQUIRED"
        assert result["field"] == "config.process_kind"

    def test_execute_component_rejects_untyped_process_update(self):
        from src.boomi_mcp.categories.integration_builder import _execute_component
        boomi_client = MagicMock()
        comp = IntegrationComponentSpec(
            key="untyped_proc",
            type="process",
            action="update",
            name="Untyped Process",
            component_id="explicit-proc-id",
            # Even with config.xml a type='process' component without
            # process_kind is rejected — raw process XML must use the
            # type='component' escape hatch instead.
            config={
                "name": "Untyped Process",
                "xml": "<bns:Component>...pre-built XML...</bns:Component>",
            },
        )
        result = _execute_component(
            boomi_client, "dev", comp, comp.config, target_id="explicit-proc-id",
        )
        assert result["_success"] is False
        assert result["error_code"] == "PROCESS_KIND_REQUIRED"

    def test_raw_process_xml_escape_hatch_via_generic_component(self):
        """The raw process XML escape hatch is type='component' + config.xml
        (config.type='process'), which routes through update_component."""
        from src.boomi_mcp.categories.integration_builder import _execute_component
        boomi_client = MagicMock()
        comp = IntegrationComponentSpec(
            key="raw_proc",
            type="component",
            action="update",
            name="Raw Process",
            component_id="explicit-proc-id",
            config={
                "type": "process",
                "xml": "<bns:Component>...pre-built XML...</bns:Component>",
            },
        )
        with patch(
            "src.boomi_mcp.categories.integration_builder.update_component"
        ) as mock_update_component:
            mock_update_component.return_value = {"_success": True}
            _execute_component(
                boomi_client, "dev", comp, comp.config, target_id="explicit-proc-id",
            )
        mock_update_component.assert_called_once()


class TestCodexR15Followups:
    """Codex r15 P3: error_unsupported_structured_update entries in
    _apply_plan unresolvable_steps must surface a non-empty error detail."""

    @patch(_PATCH_TARGET)
    def test_unsupported_structured_update_emits_actionable_error_detail(self, mock_pag):
        bad = IntegrationComponentSpec(
            key="http_conn",
            type="connector-settings",
            action="update",
            name="HTTP",
            component_id="explicit-id",
            config={
                "connector_type": "http",
                "base_url": "https://example.com",
                "auth": "NONE",
            },
        )
        config = _build_config([bad])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert len(result.get("details", [])) >= 1
        # The detail string should reference the failing component AND
        # carry the structured error_code so callers can grep.
        assert any(
            "http_conn" in d
            or "UPDATE_PRESERVATION_POLICY_UNSUPPORTED" in d
            for d in result["details"]
        )


class TestCodexR16Followups:
    """Codex r16 P2: DB Get op non-string operation_mode guard + REST op
    profile-binding preservation on path-only updates."""

    @patch(_PATCH_TARGET)
    def test_non_string_operation_mode_does_not_crash_plan(self, mock_pag):
        """Codex r16 P2: DB Get op update with body fields and a
        non-string operation_mode used to crash _build_plan via
        DatabaseGetOperationBuilder.validate_config calling .lower()
        on an int. Now it returns a clean structured envelope."""
        comp = IntegrationComponentSpec(
            key="bad_op",
            type="connector-action",
            action="update",
            name="Bad Op",
            component_id="explicit-id",
            config={
                "connector_type": "database",
                "operation_mode": 123,  # int, not str — used to crash
                "component_name": "Bad Op",
                "read_profile_id": "5fe35b85-d8f4-409d-8197-03eee5c0c129",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        # Should not raise; plan completes (validation envelope OK).
        assert plan["_success"] is True

    @patch(_PATCH_TARGET)
    def test_rest_op_policy_owned_attrs_only_contains_method(self, mock_pag):
        """Codex r16 P2 trade-off: REST op owned_attrs is narrowed to
        the always-explicit method attrs. Profile-related attrs
        (requestProfile, responseProfile, *ProfileType) are NOT in
        owned_attrs — they preserve live values on path-only updates.
        Documented limitation: explicit-clear of profile bindings via
        structured update no longer works; use raw-XML escape hatch."""
        from src.boomi_mcp.categories.components.builders.connector_builder import (
            _REST_CLIENT_OPERATION_POLICY,
        )
        cfg = next(
            op
            for op in _REST_CLIENT_OPERATION_POLICY.owned_paths
            if op.mode == "key_merge"
        )
        assert cfg.owned_attrs is not None
        # Always-emitted method attrs are owned.
        assert "customOperationType" in cfg.owned_attrs
        assert "operationType" in cfg.owned_attrs
        # Profile attrs are NOT owned (Codex r16 trade-off).
        assert "requestProfile" not in cfg.owned_attrs
        assert "requestProfileType" not in cfg.owned_attrs
        assert "responseProfile" not in cfg.owned_attrs
        assert "responseProfileType" not in cfg.owned_attrs


class TestCodexR18Followups:
    """Codex r18 P2: connection_ref_key as metadata-only + profile type
    additive semantics."""

    @patch(_PATCH_TARGET)
    def test_connection_ref_key_in_metadata_only_set(self, mock_pag):
        """Codex r18 P2: a rename payload that includes connection_ref_key
        (routing-only key, not emitted to XML) must route through
        smart-merge, not invoke the structured operation builder."""
        comp = IntegrationComponentSpec(
            key="rest_op",
            type="connector-action",
            action="update",
            name="Op",
            component_id="explicit-id",
            config={
                "connector_type": "rest",
                "operation_mode": "execute",
                "connection_ref_key": "some_conn_ref",
                "component_name": "Renamed Op",
                "description": "rename only",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert step["update_mode"] == "metadata_smart_merge"
        assert "validation_error" not in step


class TestCodexR19Followups:
    """Codex r19 P2: connector_type whitespace tolerance in plan-time guard."""

    @patch(_PATCH_TARGET)
    def test_whitespace_padded_connector_type_plans_clean(self, mock_pag):
        """Codex r19 P2: connector_type=' rest ' (with whitespace) is
        accepted by _resolve_rest_connector_type and normalized at
        apply. The plan-time guard must also strip+lowercase so it
        doesn't false-flag as UPDATE_PRESERVATION_POLICY_UNSUPPORTED."""
        comp = IntegrationComponentSpec(
            key="rest_conn",
            type="connector-settings",
            action="update",
            name="REST",
            component_id="explicit-id",
            config={
                "connector_type": " rest ",  # padded alias
                "base_url": "https://example.com",
                "auth": "NONE",
            },
        )
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        # Must not be the unsupported-update error — should resolve to
        # the REST builder and either plan clean or surface a
        # builder-specific validation error (depending on payload).
        assert step["planned_action"] != "error_unsupported_structured_update"


def test_synthesize_wrappers_non_string_map_type_does_not_raise():
    """Bug #133: a transform.map with a non-string map_type must not crash
    _synthesize_script_function_wrappers on .strip() — it should be skipped so
    the validator later reports UNSUPPORTED_TRANSFORM_ROUTE."""
    from src.boomi_mcp.models.integration_models import IntegrationSpecV1
    from src.boomi_mcp.categories.integration_builder import (
        _synthesize_script_function_wrappers,
    )

    for bad in (42, True, ["script"], {"k": "v"}):
        spec = IntegrationSpecV1(
            name="t",
            components=[
                IntegrationComponentSpec(
                    key="m", type="transform.map", action="create", name="M",
                    config={"map_type": bad, "script_mappings": [{"script_component_id": "$ref:s"}]},
                )
            ],
        )
        before = len(spec.components)
        # Must not raise.
        _synthesize_script_function_wrappers(spec)
        # Non-script (coerced) map_type → no wrapper synthesized.
        assert len(spec.components) == before


# ---------------------------------------------------------------------------
# Issue #90 — wrapper_subprocess parent + standalone Process Call
# ---------------------------------------------------------------------------

def _wrapper_parent_comp(process_calls, key="wrapper_parent", depends_on=()):
    # depends_on defaults to empty: the implicit parent->child edge is synthesized
    # at plan time from each processcall $ref, so callers need not pre-declare it.
    return IntegrationComponentSpec(
        key=key,
        type="process",
        action="create",
        name="Wrapper Parent",
        config={"process_kind": "wrapper_subprocess", "process_calls": list(process_calls)},
        depends_on=list(depends_on),
    )


def _structured_child_comp(key="main_logic", name="Main Logic"):
    # A minimal typed process child the wrapper can create and call: a
    # wrapper_subprocess that invokes a single out-of-spec process by literal
    # process_id (no in-spec deps, plans clean — see
    # test_literal_process_id_creates_no_implicit_edge). Its type is still
    # "process", so the parent's subprocess_ref ref-type check passes.
    return IntegrationComponentSpec(
        key=key,
        type="process",
        action="create",
        name=name,
        config={
            "process_kind": "wrapper_subprocess",
            "process_calls": [
                {"process_id": "99999999-9999-9999-9999-999999999999"}
            ],
        },
    )


class TestWrapperSubprocessPlan:
    """Plan-time behavior for the wrapper_subprocess parent (issue #90)."""

    @patch(_PATCH_TARGET)
    def test_parent_listed_first_builds_child_first(self, mock_pag):
        mock_pag.return_value = []
        # Parent BEFORE child in the spec — the synthesized implicit edge must
        # still order the child first.
        components = [
            _wrapper_parent_comp([{"subprocess_ref": "$ref:main_logic"}]),
            _structured_child_comp(),
        ]
        plan = _build_plan(MagicMock(), _build_config(components))
        assert plan["_success"] is True
        order = plan["execution_order"]
        assert order.index("main_logic") < order.index("wrapper_parent")
        pstep = next(s for s in plan["steps"] if s["key"] == "wrapper_parent")
        assert pstep["planned_action"] == "create"

    @patch(_PATCH_TARGET)
    def test_literal_process_id_creates_no_implicit_edge(self, mock_pag):
        mock_pag.return_value = []
        # A literal process_id targets an out-of-spec component — no synthetic
        # dependency edge, and the parent needs no in-spec child.
        parent = _wrapper_parent_comp(
            [{"process_id": "99999999-9999-9999-9999-999999999999"}],
            depends_on=(),
        )
        plan = _build_plan(MagicMock(), _build_config([parent]))
        assert plan["_success"] is True
        pstep = next(s for s in plan["steps"] if s["key"] == "wrapper_parent")
        assert pstep["planned_action"] == "create"
        # No synthetic dependency was added (depends_on stays empty).
        comp = next(c for c in plan["integration_spec"]["components"] if c["key"] == "wrapper_parent")
        assert comp["depends_on"] == []

    def _wrapper_err(self, mock_pag, process_calls, extra=None):
        mock_pag.return_value = []
        components = [_wrapper_parent_comp(process_calls)]
        components.append(extra if extra is not None else _structured_child_comp())
        plan = _build_plan(MagicMock(), _build_config(components))
        assert "steps" in plan, plan
        step = next(s for s in plan["steps"] if s["key"] == "wrapper_parent")
        return step

    @patch(_PATCH_TARGET)
    def test_missing_target_errors(self, mock_pag):
        step = self._wrapper_err(mock_pag, [{"wait": True}])
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_REF_MISSING"

    @patch(_PATCH_TARGET)
    def test_ambiguous_target_errors(self, mock_pag):
        step = self._wrapper_err(
            mock_pag,
            [{"subprocess_ref": "$ref:main_logic", "process_id": "x"}],
        )
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_REF_AMBIGUOUS"

    @patch(_PATCH_TARGET)
    def test_self_reference_errors(self, mock_pag):
        # Self-ref synthesizes no edge (so topo does not cycle); the precise
        # PROCESS_REF_SELF_REFERENCE surfaces at the preflight.
        step = self._wrapper_err(mock_pag, [{"subprocess_ref": "$ref:wrapper_parent"}])
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_REF_SELF_REFERENCE"

    @patch(_PATCH_TARGET)
    def test_not_found_errors(self, mock_pag):
        # Ref to a non-existent in-spec key synthesizes no edge (so topo does not
        # choke on an unknown dependency); PROCESS_REF_NOT_FOUND surfaces at the
        # preflight.
        step = self._wrapper_err(mock_pag, [{"subprocess_ref": "$ref:ghost"}])
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_REF_NOT_FOUND"

    @patch(_PATCH_TARGET)
    def test_ref_in_process_id_errors(self, mock_pag):
        # A $ref token in process_id (wrong field) is rejected at plan time.
        step = self._wrapper_err(mock_pag, [{"process_id": "$ref:main_logic"}])
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_CALL_CONFIG_INVALID"

    @patch(_PATCH_TARGET)
    def test_type_mismatch_errors(self, mock_pag):
        # subprocess_ref pointing at a non-process (in-spec) component.
        conn = IntegrationComponentSpec(
            key="some_conn", type="connector-settings", action="create",
            name="Conn", config={"connector_type": "database"},
        )
        step = self._wrapper_err(
            mock_pag,
            [{"subprocess_ref": "$ref:some_conn"}],
            extra=conn,
        )
        assert step["planned_action"] == "error_process_validation"
        assert step["validation_error"]["error_code"] == "PROCESS_REF_TYPE_MISMATCH"

    @patch("src.boomi_mcp.categories.integration_builder._execute_component")
    @patch(_PATCH_TARGET)
    def test_apply_creates_child_first_and_resolves_id_into_parent(self, mock_pag, mock_exec):
        # Apply-layer proof (parallel to test_apply_resolves_ref_token_to_created_profile_id):
        # with the parent listed FIRST, _apply_plan executes the child first, puts
        # its created id in the registry, and the parent's resolved config carries
        # that id in the processcall subprocess_ref (not the literal $ref token).
        mock_pag.return_value = []
        # side_effect is consumed in execution order — child (main_logic) first,
        # then the wrapper parent.
        mock_exec.side_effect = [
            {"_success": True, "component_id": "child-id-001", "type": "process"},
            {"_success": True, "component_id": "parent-id-002", "type": "process"},
        ]
        components = [
            _wrapper_parent_comp([{"subprocess_ref": "$ref:main_logic"}]),  # listed first
            _structured_child_comp(),
        ]
        config = _build_config(components)
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is True, result
        assert mock_exec.call_count == 2
        # Both components are processes; distinguish them by processcall shape —
        # the parent's call carries subprocess_ref ($ref:main_logic, resolved at
        # apply), the child's carries a literal process_id.
        configs = [c.kwargs["config"] for c in mock_exec.call_args_list]
        parent_idx = next(
            i for i, cfg in enumerate(configs)
            if "subprocess_ref" in cfg["process_calls"][0]
        )
        child_idx = next(
            i for i, cfg in enumerate(configs)
            if "process_id" in cfg["process_calls"][0]
        )
        # Child created BEFORE the parent.
        assert child_idx < parent_idx
        # The parent's resolved config carries the child's CREATED id (no $ref leak).
        parent_call = configs[parent_idx]["process_calls"][0]["subprocess_ref"]
        assert parent_call == "child-id-001"
        assert "$ref" not in parent_call

    def test_ref_resolves_into_emitted_parent_xml(self):
        # The whole $ref -> resolve -> emit path: a $ref:KEY subprocess_ref must
        # be substituted by _resolve_dependency_tokens (as integration_builder
        # does before build()) and the RESOLVED id must reach the processcall.
        import xml.etree.ElementTree as ET

        from src.boomi_mcp.categories.integration_builder import _resolve_dependency_tokens
        from src.boomi_mcp.categories.components.builders import WrapperSubprocessBuilder

        resolved_child = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        cfg = {"process_kind": "wrapper_subprocess",
               "process_calls": [{"subprocess_ref": "$ref:main_logic"}]}
        resolved_cfg = _resolve_dependency_tokens(cfg, {"main_logic": resolved_child})
        xml = WrapperSubprocessBuilder.build(resolved_cfg, name="N")
        root = ET.fromstring(xml)
        pc = root.find("bns:object/process", {"bns": "http://api.platform.boomi.com/"}) \
            .find("shapes").findall("shape")[1].find("configuration/processcall")
        assert pc.attrib["processId"] == resolved_child
        assert "$ref" not in pc.attrib["processId"]


# ============================================================================
# Issue #80 (M9.4) — process-graph integrity in build_integration verify
# ============================================================================


class TestVerifyProcessGraph:
    """Handler-level verify tests for the process-graph integrity pass.

    Seeds the in-memory build registry directly, then routes through the public
    build_integration_action verify entry with a patched component GET so the
    new graph verifier runs against fixture XML.
    """

    _FIXTURES = Path(__file__).parent / "fixtures" / "process_graph"

    def _seed_build(self, build_id, components, results):
        from src.boomi_mcp.categories.integration_builder import _BUILD_REGISTRY
        from src.boomi_mcp.models.integration_models import IntegrationSpecV1

        spec = IntegrationSpecV1(name="GraphVerify", components=components)
        _BUILD_REGISTRY[build_id] = {
            "created_at": "2026-01-01T00:00:00Z",
            "profile": "prof",
            "spec": spec.model_dump(),
            "results": results,
            "execution_order": [c.key for c in components],
        }

    def _fixture(self, name):
        return (self._FIXTURES / name).read_text(encoding="utf-8")

    def test_graph_error_flips_success_false(self):
        from src.boomi_mcp.categories.integration_builder import build_integration_action

        comp = _comp(key="p1", name="Proc", comp_type="process")
        self._seed_build("graph-err", [comp], {"p1": {"component_id": "id-p1"}})
        err_xml = self._fixture("non_terminal_no_outbound.xml")
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            return_value={"type": "process", "xml": err_xml},
        ):
            result = build_integration_action(
                MagicMock(), "prof", "verify", {"build_id": "graph-err"}
            )
        assert result["_success"] is False, result
        assert result["failed_components"] >= 1
        record = result["verification"]["p1"]
        assert record["verified"] is False
        assert record["error_code"] == "PROCESS_GRAPH_INTEGRITY_FAILED"
        assert record["reason"] == "Process graph integrity errors"
        assert record["process_graph"]["errors"]
        assert "NON_TERMINAL_SHAPE_DEAD_END" in {
            e["code"] for e in record["process_graph"]["errors"]
        }

    def test_graph_warning_only_keeps_success_true(self):
        from src.boomi_mcp.categories.integration_builder import build_integration_action

        comp = _comp(key="p1", name="Proc", comp_type="process")
        self._seed_build("graph-warn", [comp], {"p1": {"component_id": "id-p1"}})
        # A display-attribute lint is warning-only (issue #102 promoted the bare
        # <stop/> STOP_CONTINUE_MISSING lint to a hard error, so this verifies the
        # warning-only path with a still-advisory fixture).
        warn_xml = self._fixture("missing_display_attrs.xml")
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            return_value={"type": "process", "xml": warn_xml},
        ):
            result = build_integration_action(
                MagicMock(), "prof", "verify", {"build_id": "graph-warn"}
            )
        assert result["_success"] is True, result
        assert result["failed_components"] == 0
        record = result["verification"]["p1"]
        assert record["verified"] is True
        assert "error_code" not in record
        assert record["process_graph"]["errors"] == []
        assert record["process_graph"]["warnings"]
        assert "DISPLAY_ATTRIBUTE_MISSING" in {
            w["code"] for w in record["process_graph"]["warnings"]
        }

    def test_valid_process_graph_attaches_clean_section(self):
        from src.boomi_mcp.categories.integration_builder import build_integration_action

        comp = _comp(key="p1", name="Proc", comp_type="process")
        self._seed_build("graph-ok", [comp], {"p1": {"component_id": "id-p1"}})
        ok_xml = self._fixture("valid_linear_process.xml")
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            return_value={"type": "process", "xml": ok_xml},
        ):
            result = build_integration_action(
                MagicMock(), "prof", "verify", {"build_id": "graph-ok"}
            )
        assert result["_success"] is True, result
        record = result["verification"]["p1"]
        assert record["verified"] is True
        assert record["process_graph"] == {
            "errors": [],
            "warnings": [],
            "shapes_checked": 3,
        }

    def test_non_process_component_has_no_process_graph(self):
        from src.boomi_mcp.categories.integration_builder import build_integration_action

        comp = _comp(key="c1", name="Conn", comp_type="connector-settings")
        self._seed_build("non-proc", [comp], {"c1": {"component_id": "id-c1"}})
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            return_value={"type": "connector-settings", "xml": "<bns:Component/>"},
        ):
            result = build_integration_action(
                MagicMock(), "prof", "verify", {"build_id": "non-proc"}
            )
        assert result["_success"] is True, result
        record = result["verification"]["c1"]
        assert record["verified"] is True
        assert "process_graph" not in record

    def test_process_with_no_usable_xml_does_not_silently_pass(self):
        """A detected process whose GET yields no usable XML must surface a
        failing process_graph, not verify clean unverified."""
        from src.boomi_mcp.categories.integration_builder import build_integration_action

        comp = _comp(key="p1", name="Proc", comp_type="process")
        self._seed_build("graph-noxml", [comp], {"p1": {"component_id": "id-p1"}})
        with patch(
            "src.boomi_mcp.categories.integration_builder.component_get_xml",
            return_value={"type": "process", "xml": None},
        ):
            result = build_integration_action(
                MagicMock(), "prof", "verify", {"build_id": "graph-noxml"}
            )
        assert result["_success"] is False, result
        record = result["verification"]["p1"]
        assert record["verified"] is False
        assert record["error_code"] == "PROCESS_GRAPH_INTEGRITY_FAILED"
        assert record["process_graph"]["errors"]
