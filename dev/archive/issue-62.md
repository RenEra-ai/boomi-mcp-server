**Title: Issue #62 Runtime Attachment And Schedule Activation**

**Summary**
Add the M3.3 stage inside `orchestrate_deploy_action` only. Keep the tool internal, reuse existing action routers, and preserve `dry_run=True` as the no-SDK default. Real runs must complete package/deploy first, then runtime/process bindings, then schedule activation. Any failure blocks later stages with structured errors.

**File-By-File Plan**

`src/boomi_mcp/categories/deployment/orchestration.py`

- Add imports:
  - `from ..environments import manage_environments_action`
  - `from ..runtimes import manage_runtimes_action`
  - `from ..schedules import manage_schedules_action`
  - Keep the existing relative registry import unchanged.
- Extend `StageStatus` with `attached`, `updated`, `enabled`, and `disabled`.
- Add structured error constants:
  - `ENVIRONMENT_VERIFY_FAILED`
  - `RUNTIME_VERIFY_FAILED`
  - `RUNTIME_ENV_ATTACHMENT_LIST_FAILED`
  - `RUNTIME_ENV_ATTACHMENT_CREATE_FAILED`
  - `RUNTIME_ENV_ATTACHMENT_ID_MISSING`
  - `PROCESS_ENV_ATTACHMENT_LIST_FAILED`
  - `PROCESS_ENV_ATTACHMENT_CREATE_FAILED`
  - `PROCESS_ENV_ATTACHMENT_ID_MISSING`
  - `PROCESS_RUNTIME_ATTACHMENT_LIST_FAILED`
  - `PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED`
  - `PROCESS_RUNTIME_ATTACHMENT_ID_MISSING`
  - `SCHEDULE_OVERRIDE_INVALID`
  - `SCHEDULE_UPDATE_FAILED`
  - `SCHEDULE_DELETE_FAILED`
  - `SCHEDULE_ENABLE_FAILED`
  - `SCHEDULE_DISABLE_FAILED`
  - `SCHEDULE_ID_MISSING`
- Keep `OrchestrateDeployRequest` fields as currently wired: `build_id`, `environment_id`, `runtime_id`, `profile`, `schedule_override`, `run_test`, `dry_run`, `package_version`. Add content validation for `schedule_override` after build resolution, not in the Pydantic type itself.
- Expand `RuntimeAttachmentStage` while preserving existing `attachment_id`:
  - `status`, `attachment_id`, `runtime_id`, `environment_id`, `process_id`
  - `environment_atom_attachment_id`, `environment_atom_attachment_status`
  - `process_environment_attachment_id`, `process_environment_attachment_status`
  - `process_atom_attachment_id`, `process_atom_attachment_status`
  - `reused`, `changed`, `warnings`
- Expand `ScheduleStage`:
  - `status`, `schedule_id`, `schedule_status_id`, `schedule_override`
  - `process_id`, `runtime_id`, `environment_id`
  - `cron`, `max_retry`, `enabled`
  - `reused`, `changed`, `warnings`
- Add helper wrappers matching the existing deployment wrapper style:
  - `_call_environment_action(boomi_client, profile, action, config_data) -> Dict[str, Any]`
  - `_call_runtime_action(boomi_client, profile, action, config_data) -> Dict[str, Any]`
  - `_call_schedule_action(boomi_client, profile, action, config_data) -> Dict[str, Any]`
- Add runtime binding stage:
  - Signature:
    ```python
    def _ensure_runtime_attachment(
        boomi_client: Any,
        profile: Optional[str],
        *,
        process_id: str,
        environment_id: str,
        runtime_id: str,
    ) -> Tuple[Optional[RuntimeAttachmentStage], Optional[OrchestrateDeployError]]:
    ```
  - Call `manage_environments_action(..., action="get", config_data={"resource_id": environment_id})`.
  - Call `manage_runtimes_action(..., action="get", config_data={"resource_id": runtime_id})`.
  - Call `manage_runtimes_action(..., action="list_attachments", config_data={"environment_id": environment_id})`; reuse an attachment whose `atom_id == runtime_id`, otherwise call `action="attach"` with `{"resource_id": runtime_id, "environment_id": environment_id}`. This uses Boomi `EnvironmentAtomAttachment`.
  - Call `manage_deployment_action(..., action="list_process_environment_attachments", config_data={"process_id": process_id})`; reuse matching `environment_id`, otherwise call `attach_process_environment`.
  - Call `manage_deployment_action(..., action="list_process_atom_attachments", config_data={"process_id": process_id})`; reuse matching `atom_id == runtime_id`, otherwise call `attach_process_atom`.
  - Stage status is `reused` if all bindings existed, `attached` if any binding was created, `failed` on helper failure. Set `changed=True` only when an attach call succeeded.
- Add schedule override normalization:
  - Signature:
    ```python
    def _normalize_schedule_override(
        schedule_override: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[OrchestrateDeployError]]:
    ```
  - Supported inputs:
    - `None`: no schedule mutation.
    - `{"cron": "0 9 * * *"}`: scheduled shorthand, enabled by default.
    - `{"mode": "scheduled", "cron": "...", "enabled": true|false, "max_retry": 0..5}`.
    - `{"mode": "manual"}` or `{"mode": "disabled"}` or `{"enabled": false}` without cron: clear/disable scheduling.
  - Reject unknown modes, missing cron for scheduled mode, non-5-part cron, non-bool `enabled`, non-int/out-of-range `max_retry`, and unsupported keys with `SCHEDULE_OVERRIDE_INVALID`.
- Add schedule stage:
  - Signature:
    ```python
    def _apply_schedule_override(
        boomi_client: Any,
        profile: Optional[str],
        *,
        process_id: str,
        environment_id: str,
        runtime_id: str,
        schedule_override: Optional[Dict[str, Any]],
    ) -> Tuple[ScheduleStage, Optional[OrchestrateDeployError]]:
    ```
  - If normalized override is `None`, return `ScheduleStage(status="not_required", changed=False, reused=False)` and make no schedule calls.
  - For manual/disabled, call `manage_schedules_action(..., action="delete", config_data={"process_id": process_id, "atom_id": runtime_id})`, then `action="disable"` with the same IDs. Return status `disabled`.
  - For scheduled, call `action="update"` with `process_id`, `atom_id`, `cron`, and optional `max_retry`. Then call `enable` unless `enabled is False`; call `disable` when `enabled is False`. Return status `updated`, `enabled`, or `disabled` based on the final status action.
  - Populate `schedule_id` from returned `schedule["id"]` or status `id`, and `schedule_status_id` / `enabled` from returned status. Missing IDs after a successful mutation return `SCHEDULE_ID_MISSING`.
- Update response assembly:
  - `_stage_summary` should accept package, deployment, runtime attachment, and schedule stages.
  - Add summary keys:
    - `runtime_id`, `environment_id`
    - `runtime_attachment_id`, `runtime_attachment_status`
    - `schedule_id`, `schedule_status`, `schedule_enabled`
    - `resource_reuse`
    - `resource_changes`
    - extend `stage_warnings` with `runtime_attachment` and `schedule`.
- Update control flow in `orchestrate_deploy_action`:
  - Required input validation and build resolution remain before any SDK calls.
  - Validate `schedule_override` content before dry-run response.
  - `dry_run=True`: no environment/runtime/deployment attachment/schedule calls; runtime remains `planned`, schedule is `planned` if override exists or `not_required` if absent.
  - `dry_run=False`: package stage, deploy stage, runtime binding stage, schedule stage, in that order.
  - Schedule activation must only run after deploy success and runtime binding success.
  - Runtime failure returns `_success=False`, runtime stage `failed`, schedule/execution/logs/cleanup `blocked`.
  - Schedule failure returns `_success=False`, schedule stage `failed`, execution/logs/cleanup `blocked`.

`src/boomi_mcp/categories/deployment/packages.py`

- No changes. Reuse existing `manage_deployment_action` for package/deploy and process attachment helpers.

`src/boomi_mcp/categories/runtimes.py`

- No changes. Reuse `manage_runtimes_action("get")`, `list_attachments`, and `attach`.

`src/boomi_mcp/categories/environments.py`

- No changes. Reuse `manage_environments_action("get")`.

`src/boomi_mcp/categories/schedules.py`

- No changes. Reuse `manage_schedules_action("update")`, `delete`, `enable`, and `disable`.

`server.py` and `src/boomi_mcp/categories/deployment/__init__.py`

- No changes. `orchestrate_deploy_action` remains internal and already exported from the deployment package.

**Tests In `tests/test_orchestrate_deploy_contract.py`**

- Extend the fake action recorder so one fake can return queued responses per action and record `action` plus `config_data`.
- Patch `orchestration.manage_environments_action`, `manage_runtimes_action`, `manage_deployment_action`, and `manage_schedules_action` at the orchestration call site.
- Update existing real-run #61 tests to provide default successful runtime/process bindings.
- Update `test_full_success_contract` and dry-run tests for the expanded runtime/schedule stage and summary keys.

Acceptance mapping:

- Already-attached runtime: `test_real_run_reuses_existing_runtime_bindings_before_schedule`
- Attach runtime: `test_real_run_creates_missing_runtime_and_process_bindings`
- Attachment API failure: `test_runtime_attachment_api_failure_is_structured_and_blocks_schedule`
- Missing runtime/environment: `test_missing_runtime_or_environment_blocks_runtime_stage`
- Manual/no schedule: `test_schedule_override_none_or_manual_has_expected_schedule_calls`
- Schedule update: `test_schedule_override_updates_schedule_after_runtime_binding`
- Schedule enable/disable: `test_schedule_override_enable_disable_status_flows`
- Invalid schedule override: `test_invalid_schedule_override_returns_structured_error_without_schedule_call`
- Schedule API failure: `test_schedule_api_failure_is_structured_and_blocks_execution`
- Dry-run guard: `test_dry_run_with_schedule_override_never_calls_runtime_or_schedule_helpers`
- Ordering guard: `test_deploy_failure_blocks_runtime_and_schedule_without_schedule_calls`

**Implementation Validation**

- During implementation, run focused unit tests for `tests/test_orchestrate_deploy_contract.py`, then follow the repo completion workflow exactly: QA agent live `.fn()` validation, QA-clean commit, Codex review, and fix/re-review loops until both report zero issues.
