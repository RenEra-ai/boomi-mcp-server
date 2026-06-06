# Issue #63 Run Test Stage Plan

## Summary
Extend the existing internal `orchestrate_deploy_action` in [orchestration.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/deployment/orchestration.py:1460) only. Reuse the existing package/deploy/runtime/schedule flow, then replace the current M3.4 execution/log placeholders with a real optional `run_test` stage after schedule success. Do not wire a public MCP tool and do not add dependencies.

Grounded helpers to reuse:
- Execution: `execute_process_action` in [execution.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/execution.py:357), with `wait=True` polling via `_await_execution_completion` at [execution.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/execution.py:122).
- Monitoring/logs/artifacts: `monitor_platform_action` in [monitoring.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/monitoring.py:1910), `execution_logs` at [monitoring.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/monitoring.py:202), `execution_artifacts` at [monitoring.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/monitoring.py:281).

## File Changes

### [orchestration.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/deployment/orchestration.py:38)
Modify only this source file.

- Imports: add `execute_process_action` and `monitor_platform_action` beside existing sibling router imports.
- `StageStatus` around line 44: add execution/log statuses `completed`, `warning`, `timeout`, `retrieved`, `unavailable`.
- Error constants after schedule codes: add `TEST_EXECUTION_FAILED`, `TEST_EXECUTION_TIMEOUT`, `TEST_REQUEST_ID_MISSING`.
- Constants: add `_RUN_TEST_TIMEOUT_SECONDS = 300`, `_RUN_TEST_LOG_LEVEL = "ALL"`, `_RUN_TEST_LOG_EXCERPT_LINES = 80`, `_RUN_TEST_LOG_EXCERPT_CHARS = 8000`, `_RUN_TEST_LOG_MAX_FILES = 3`. Poll interval/max-polls stay delegated to `execute_process_action`: 2s until 30s, then 5s, bounded by timeout.
- `OrchestrateDeployRequest` around line 112: add:
  - `test_timeout_seconds: int = 300`
  - `test_dynamic_properties: Optional[Dict[str, Any]] = None`
  - `test_process_properties: Optional[Dict[str, Dict[str, Any]]] = None`
  - `test_log_level: str = "ALL"`
  - `test_fetch_logs: bool = True`
  - `test_fetch_artifacts: bool = True`
  - `test_log_fetch_content: bool = True`
- `orchestrate_deploy_action` signature around line 1460: add the same user-facing params after `package_version`; add optional internal `creds: Optional[Dict[str, str]] = None` after them, not included in `OrchestrateDeployRequest`.
- Expand `ExecutionStage` around line 218 with `request_id`, `terminal_status`, `poll_status`, `elapsed_seconds`, `poll_count`, `process_id`, `environment_id`, `atom_id`, `document_counts`, `error`, and `warnings`.
- Expand `LogsStage` around line 224 with `execution_id`, `log_level`, `status_code`, `message`, `download_url`, `downloaded`, `log_excerpts`, `artifact_status`, `artifact_status_code`, `artifact_message`, `artifact_download_url`, `error`, `artifact_error`, and `warnings`.
- Add wrappers after `_call_schedule_action` around line 523:
  - `_call_execute_process_action(...)`
  - `_call_monitor_action(...)`
- Add helpers after schedule helpers, before response assembly:
  - `_build_test_execution_config(...)`: forces `wait=True`, sets timeout, passes dynamic/process properties.
  - `_execution_stage_from_result(...)`: maps `COMPLETE -> completed`, `COMPLETE_WARN -> warning`, `ERROR/ABORTED -> failed`, `TIMEOUT -> timeout`, preserving request/execution IDs, elapsed/poll count, document counts, and error text.
  - `_bounded_log_excerpts(files)`: first 3 files, max 80 lines and 8000 chars per excerpt.
  - `_logs_stage_from_results(...)`: normalizes log/artifact monitor results; log failure becomes `unavailable` but is diagnostic, not a test failure.
  - `_run_test_stage(...)`: execute, normalize, fetch logs/artifact metadata when `execution_id` exists.
  - `_real_run_with_test_response(...)`: mirrors `_real_run_response`, but accepts concrete `execution` and `logs` stages.
- `_stage_summary` around line 1190: keep existing fields and add `summary["test"]` with `run_test`, request ID, execution ID, execution stage status, terminal status, poll status, elapsed seconds, poll count, document counts, execution error, logs status, log download URL, log excerpt count, artifact download URL, log/artifact errors.
- Handler flow after schedule success around line 1688:
  - If `run_test=False`, keep current `_real_run_response`.
  - If `run_test=True`, call `_run_test_stage`.
  - If execution terminal is `ERROR`, `ABORTED`, timeout, missing request ID, or execute helper failure: return `_success=False` with package/deploy/runtime/schedule preserved, concrete execution/logs stages, cleanup `not_required`, and the matching test error.
  - If terminal is `COMPLETE_WARN`: return `_success=True`, `execution.status="warning"`, summary warning fields populated.
  - If log/artifact fetch fails after execution success: return `_success=True`, `logs.status="unavailable"`, and surface the failure in `logs` plus `summary["test"]`.

Dry-run and failure short-circuit rules:
- `dry_run=True`: no execute/log/artifact calls; existing `planned`/`skipped` placeholders remain.
- Package/deploy failure: existing `_blocked_real_run_response` continues to block execution/logs.
- Runtime/schedule failure: existing `_runtime_or_schedule_failed_response` continues to block execution/logs.
- Execution/log retrieval never runs unless package, deploy, runtime attachment, and schedule all succeeded.

### [test_orchestrate_deploy_contract.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_orchestrate_deploy_contract.py:1)
Modify only this test file.

Add fake helpers near existing `_FakeAction`:
- `_FakeExecuteAction`: records `process_id`, `environment_id`, `atom_id`, `config_data`, returns canned execution dicts.
- `_patch_test_actions(monkeypatch, execute_result, monitor_responses=None, order_log=None)`: patches `orchestration.execute_process_action` and `orchestration.monitor_platform_action`.
- Small builders for execution results: `_exec_complete`, `_exec_warn`, `_exec_failed`, `_exec_timeout`.

Add these handler tests:
- `test_run_test_false_real_run_skips_execution_and_logs`: successful real deploy with `run_test=False`; asserts execute/monitor fakes are not called, execution/logs are `skipped`.
- `test_run_test_dry_run_plans_without_execution_or_log_calls`: `dry_run=True, run_test=True`; asserts no execute/monitor calls, execution/logs are `planned`.
- `test_run_test_success_executes_after_schedule_and_fetches_diagnostics`: asserts call order after schedule, `execute_process_action` receives process ID, environment ID, runtime ID as atom ID, `wait=True`, timeout 300, and response includes request ID, execution ID, terminal `COMPLETE`, elapsed/poll count, document counts, log URL/excerpts, artifact URL.
- `test_run_test_complete_warn_is_success_with_warning_summary`: fake terminal `COMPLETE_WARN`; asserts top-level `_success=True`, execution status `warning`, terminal status in stage and summary.
- `test_run_test_error_and_aborted_fail_with_terminal_details`: parametrize `ERROR` and `ABORTED`; asserts top-level `_success=False`, `TEST_EXECUTION_FAILED`, error text, document counts, and logs attempted when execution ID exists.
- `test_run_test_timeout_fails_and_does_not_fetch_logs_without_execution_id`: fake timeout; asserts `_success=False`, `TEST_EXECUTION_TIMEOUT`, elapsed/poll count, logs unavailable/skipped, monitor not called.
- `test_run_test_no_execution_id_skips_log_fetch_but_keeps_request_id`: terminal completion with request ID but no execution ID; asserts request ID present, execution ID null, monitor not called, summary surfaces missing log pointer.
- `test_run_test_log_fetch_success_is_bounded`: monitor returns oversized `files`; asserts excerpts are capped by line/char/file constants and download URL remains present.
- `test_run_test_log_fetch_unavailable_is_diagnostic_not_execution_failure`: execution complete, log helper returns `_success=False`; asserts top-level success remains true, logs status `unavailable`, summary contains log error.
- `test_run_test_dynamic_and_process_properties_pass_through`: passes `test_dynamic_properties` and `test_process_properties`; asserts execute fake receives them unchanged in `config_data`.
- `test_prior_stage_failures_block_run_test_without_execute_or_monitor`: parametrize package, deploy, runtime, and schedule failures using existing fakes; asserts execution/logs `blocked` and no execute/monitor calls.

## Verification Plan
After implementation, run focused unit tests for `tests/test_orchestrate_deploy_contract.py`, then the relevant wider suite. Per repo rules, completion then requires live `boomi-qa-tester` `.fn()` validation, a QA-clean commit, and Codex review with zero issues before reporting done.

## Assumptions
- No server/public MCP wiring changes; issue #64 owns public exposure.
- Log content download depends on existing monitoring behavior and optional internal `creds`; without creds, the summary still returns download pointers.
- Log retrieval failure is diagnostic and does not turn a `COMPLETE`/`COMPLETE_WARN` test into a failed orchestration.
