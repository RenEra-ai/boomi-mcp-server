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
    def test_custom_driver_id_marks_plan_as_database_validation_error(self, mock_pag):
        mock_pag.return_value = []
        comp = _db_comp(driver_id="custom")
        config = _build_config([comp])
        plan = _build_plan(MagicMock(), config)
        step = plan["steps"][0]
        assert step["planned_action"] == "error_database_validation"
        assert step["validation_error"]["error_code"] == "UNSUPPORTED_DB_DRIVER_SHAPE"
        assert step["validation_error"]["field"] == "driver_id"

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
    def test_update_path_skips_pooling_validation_but_keeps_secret_scan(self, mock_pag):
        """Update mode (component_id supplied) bypasses full builder validation
        — bogus pooling key does NOT block update, but plaintext secrets still do.
        Same boundary applies to reuse — the will_invoke_builder gate only fires
        for create/create_clone in _build_plan."""
        mock_pag.return_value = []
        # Bogus pooling on update path: should NOT trip shape/pooling validation.
        comp_update = IntegrationComponentSpec(
            key="db_update", type="connector-settings", action="update",
            name="Existing DB", component_id="existing-db-id",
            config={"connector_type": "database",
                    "pooling": {"totally_bogus_key": True}},
        )
        plan_update = _build_plan(MagicMock(), _build_config([comp_update]))
        step_update = plan_update["steps"][0]
        assert step_update["planned_action"] != "error_database_validation"

        # But plaintext secret on update path is STILL caught by the secret scan.
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
    def test_apply_fails_before_execution_on_unsupported_db_driver_shape(self, mock_pag, mock_exec):
        mock_pag.return_value = []
        config = _build_config([_db_comp(driver_id="custom")])
        config["dry_run"] = False
        result = _apply_plan(MagicMock(), "dev", config)
        assert result["_success"] is False
        assert len(result["unresolvable_steps"]) == 1
        bad = result["unresolvable_steps"][0]
        assert bad["planned_action"] == "error_database_validation"
        assert bad["validation_error"]["error_code"] == "UNSUPPORTED_DB_DRIVER_SHAPE"
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
    def test_update_path_skips_rest_builder_preflight(self, mock_pag):
        comp = _rest_conn_comp(action="update")
        comp.component_id = "explicit-rest-id"
        comp.config.pop("base_url")
        plan = _build_plan(MagicMock(), _build_config([comp]))
        step = plan["steps"][0]
        assert step["planned_action"] == "update"
        assert "validation_error" not in step

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
