# M3.6 Orchestrate Deploy Hardening Plan

## Summary
Harden `orchestrate_deploy` by extending the existing orchestration response machinery, not by adding a parallel flow. Keep all changes dependency-free and scoped to the orchestration engine, wrapper forwarding/docs, capability metadata, and contract tests.

Core behavior:
- Stage failures expose `error_code`, `failed_stage`, `prior_stage_summary`, and `next_step`.
- Retry safety is preserved by reusing existing package/deploy/attachment helpers and adding clearer reuse/change summary fields for package and deployment.
- Cleanup becomes real planning: failed real runs return an explicit dry-run cleanup plan by default, naming exact low-level undeploy/delete/detach/schedule operations. Destructive cleanup only runs with explicit `cleanup_on_failure=true`.

## File-By-File Changes

### `src/boomi_mcp/categories/deployment/orchestration.py`
Line areas: constants/models near `~82-287`, error helpers near `~322`, run-test/log helpers near `~1441`, response assembly near `~1603-1943`, main action near `~1952-2231`.

- Add request input:
  - `cleanup_on_failure: StrictBool = False` to `OrchestrateDeployRequest`.
  - Add `cleanup_on_failure: bool = False` to `orchestrate_deploy_action(...)`, thread it into the request, and use it only on failed real-run paths.
  - Import `StrictBool` from `pydantic`; no new dependency.

- Add/extend response models:
  - Extend `LogsStage` with `error_code`, `failed_stage`, and `next_step` for diagnostic log retrieval failures.
  - Add `artifact_error_code`, `artifact_failed_stage`, and `artifact_next_step` for artifact retrieval failures.
  - Replace placeholder `CleanupStage` with:
    - `status`
    - `cleanup_id`
    - `dry_run: bool = True`
    - `mutation_allowed: bool = False`
    - `operations: List[CleanupOperation]`
    - `results: List[Dict[str, Any]]`
    - `warnings`
    - `next_step`
  - Add `CleanupOperation` model with: `tool`, `action`, `resource_type`, `resource_id`, `config`, `reason`, `destructive=True`.

- Add constants:
  - `LOG_RETRIEVAL_FAILED`
  - `ARTIFACT_RETRIEVAL_FAILED`
  - Cleanup result codes only if applying cleanup can fail, e.g. `CLEANUP_OPERATION_FAILED`.

- Add centralized failure metadata helpers near `_error(...)`:
  - `_failed_stage_for_error_code(code) -> str`
  - `_next_step_for_failure(error, failed_stage, summary) -> str`
  - `_prior_stage_summary(failed_stage, package, deployment, runtime_attachment, schedule, execution=None, logs=None) -> Dict[str, Any]`
  - Map:
    - package/client/package errors -> `package`
    - deploy errors -> `deployment`
    - env/runtime/process attachment errors -> `runtime_attachment`
    - schedule mutation errors -> `schedule`
    - test timeout/failed/missing request id -> `execution`
    - log diagnostics -> `logs`

- Extend `_stage_summary(...)`:
  - Add package/deployment to `resource_reuse` and `resource_changes`.
  - Add `stage_statuses` for package, deployment, runtime attachment, schedule, and optionally execution/logs.
  - Preserve existing keys.

- Implement cleanup planning helpers near response assembly:
  - `_cleanup_operations_for_failure(...)`
  - `_cleanup_stage_for_failure(..., cleanup_on_failure)`
  - Operation order is reverse creation order:
    1. schedule `delete` / `disable` when `schedule.changed is True`
    2. `detach_process_atom` for process-runtime attachments created by this run
    3. `detach_process_environment` for process-env attachments created by this run
    4. runtime `detach` for runtime-env attachments created by this run
    5. `undeploy` for deployments created by this run
    6. `delete_package` for packages created by this run
  - Only include resources created/changed by this orchestration attempt:
    - `package.status == "created"`
    - `deployment.status == "deployed"`
    - attachment leg status == `"attached"`
    - `schedule.changed is True`
  - Default: `status="planned"`, `dry_run=True`, no cleanup action calls.
  - Explicit opt-in: if `cleanup_on_failure is True`, execute operations through existing `_call_*_action` wrappers and record results.

- Modify failed response builders:
  - `_blocked_real_run_response(...)`
  - `_runtime_or_schedule_failed_response(...)`
  - `_real_run_with_test_response(...)` when `success=False`
  - Add top-level `error_code`, `failed_stage`, `prior_stage_summary`, and `next_step`.
  - Replace cleanup placeholder with the cleanup plan/apply result.

- Modify `_logs_stage_from_results(...)`:
  - Log/artifact retrieval failures remain diagnostic and do not flip `_success` to false.
  - Populate structured log/artifact error codes plus next-step hints on the `logs` stage and in `summary["test"]`.

### `server.py`
Line areas: `_ORCH_CONFIG_KEYS` and wrapper around `~2840-3120`.

- Add `"cleanup_on_failure"` to `_ORCH_CONFIG_KEYS`.
- Validate `cleanup_on_failure` from config is a real bool before any credential path; reject non-bool with `INVALID_CONFIG_TYPE`.
- Forward `cleanup_on_failure` into `orchestrate_deploy_action`.
- Update docstring allowed config keys and response description.
- In `_orchestrate_next_steps`, if a failed result has top-level `next_step`, include it first in `next_steps`.

### `src/boomi_mcp/categories/meta_tools.py`
Line areas: `orchestrate_deploy` capability around `~5651`, workflow text around `~6260`.

- Update `orchestrate_deploy` capability description to mention failure metadata and dry-run cleanup planning.
- Add `cleanup_on_failure` to allowed config keys.
- Add `cleanup`, `error_code`, `failed_stage`, `prior_stage_summary`, and `next_step` to response keys.
- Keep workflow wording deployment-before-schedule/test.

### `src/boomi_mcp/categories/deployment/__init__.py`
No code change expected. Keep current export of `orchestrate_deploy_action`.

## New Contract Tests

Add to `tests/test_orchestrate_deploy_contract.py`:

- `test_deploy_failure_includes_failure_metadata_and_cleanup_plan`
  - Deploy fails after package creation.
  - Assert `error_code=="DEPLOY_CREATE_FAILED"`, `failed_stage=="deployment"`, prior summary contains package only, cleanup plans `delete_package`, and no destructive cleanup call occurred.

- `test_attach_failure_includes_failure_metadata_and_partial_summary`
  - Runtime/process attachment fails after prior package/deploy success.
  - Assert failed stage `runtime_attachment`, prior summary includes package/deployment, partial attachment IDs remain visible, and cleanup names only newly attached legs.

- `test_schedule_failure_includes_failure_metadata_and_prior_summary`
  - Schedule update or enable/disable fails.
  - Assert failed stage `schedule`, prior summary includes package/deploy/runtime attachment, and `next_step` tells caller to fix schedule issue and retry same call.

- `test_execution_timeout_includes_failure_metadata_and_prior_summary`
  - `run_test=True`, execution timeout.
  - Assert `TEST_EXECUTION_TIMEOUT`, failed stage `execution`, prior summary includes package/deploy/runtime/schedule, logs remain blocked, cleanup plan is dry-run.

- `test_log_retrieval_failure_is_structured_diagnostic_with_next_step`
  - Execution completes but log fetch fails.
  - Assert overall `_success is True`, `logs.status=="unavailable"`, `logs.error_code=="LOG_RETRIEVAL_FAILED"`, `logs.failed_stage=="logs"`, and a monitor-platform retry hint is present.

- `test_retry_after_partial_attachment_failure_reuses_prior_successes`
  - First call creates runtime-env and process-env attachments, then process-runtime attach fails.
  - Second call simulates those attachments already existing and asserts no duplicate attach calls for them; only missing process-runtime attachment is created.

- `test_repeated_call_with_existing_resources_does_not_duplicate_package_deploy_attach_or_schedule`
  - Existing package, active deployment, all attachments, and deterministic schedule update path.
  - Assert no `create_package`, no `deploy`, no attach calls; schedule uses update/enable only, never a duplicate create path.

- `test_cleanup_plan_dry_run_names_exact_operations_without_mutation`
  - Failure after package, deployment, attachments, and schedule changed.
  - Assert cleanup operations exactly list schedule delete/disable, process-runtime detach, process-env detach, runtime-env detach, undeploy, delete_package in that order.
  - Assert destructive action names were not called unless `cleanup_on_failure=True`.

Also update existing assertions that currently expect `cleanup.status=="blocked"` on failures to expect `planned` when cleanup operations exist and `not_required` when there is nothing safe to clean up.

## Wrapper And Capability Tests

- Add wrapper coverage in `tests/test_orchestrate_deploy_wrapper.py`:
  - `test_cleanup_on_failure_config_forwarded_to_engine`
  - `test_invalid_cleanup_on_failure_in_config_fails_closed`
- Update capability tests that assert allowed config/response keys if present in `tests/test_meta_tools_list_capabilities.py` or wrapper capability tests.

## Verification Plan

Run focused tests first:
- `python -m pytest tests/test_orchestrate_deploy_contract.py tests/test_orchestrate_deploy_wrapper.py -q`

Run regression targets:
- `python -m pytest tests/test_manage_deployment_wrapper.py tests/test_build_integration_wrapper.py tests/test_execute_process_bug24.py tests/test_execute_process_bug38.py tests/test_meta_tools_list_capabilities.py tests/test_list_capabilities_wrapper.py -q`
- `python -m pytest tests -q`

Completion workflow after implementation:
- Run `boomi-qa-tester` per repo instructions.
- Commit the QA-clean baseline.
- Run Codex review with the repo’s companion command, then fix/retest/re-review until zero issues.

## Assumptions

- No new dependencies.
- No cleanup of built Boomi components is added; cleanup is limited to packages, deployments, attachments, and schedules touched by orchestration.
- Cleanup defaults to dry-run planning and never mutates unless `cleanup_on_failure=true`.
- Boomi docs KB returned `warming_up` during planning, so this plan relies only on the repo’s existing sibling tool contracts for cleanup actions.
