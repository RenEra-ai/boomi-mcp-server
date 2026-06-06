# Issue #65 — Claude Implementation Plan (M3.6 Harden orchestration failures, idempotency, cleanup planning)

## Summary

Issue #65 hardens `orchestrate_deploy` for failure modes, idempotency, and cleanup planning by extending the existing orchestration engine in `src/boomi_mcp/categories/deployment/orchestration.py`, threading a new `cleanup_on_failure` flag through `server.py`, and updating capability metadata in `meta_tools.py`. Core work: every failed terminal path gains top-level `error_code`, `failed_stage`, `prior_stage_summary`, and `next_step`; the `CleanupStage` placeholder becomes a real dry-run cleanup plan that names exact undeploy/delete/detach/schedule operations (created/changed resources only) and defaults to no mutation; and a new suite of contract/wrapper tests covers the acceptance scenarios. Anchor corrections vs the architect plan: `dry_run`/`run_test` use plain `bool` (not `StrictBool`), so keep `cleanup_on_failure: bool`; three distinct failed-response builders plus the no-test success-failure path in `_real_run_with_test_response`.

## File-by-file

### `src/boomi_mcp/categories/deployment/orchestration.py`

**1. New error-code constants (after line 114).**
```python
# Failure-hardening + cleanup codes (issue #65).
LOG_RETRIEVAL_FAILED = "LOG_RETRIEVAL_FAILED"
ARTIFACT_RETRIEVAL_FAILED = "ARTIFACT_RETRIEVAL_FAILED"
CLEANUP_OPERATION_FAILED = "CLEANUP_OPERATION_FAILED"
```
`LOG_RETRIEVAL_FAILED`/`ARTIFACT_RETRIEVAL_FAILED` are diagnostic (do NOT flip `_success`). `CLEANUP_OPERATION_FAILED` only used when `cleanup_on_failure=True` and a cleanup call fails.

**2. Request field (`OrchestrateDeployRequest`, lines 129-147).** Add after `package_version`:
```python
cleanup_on_failure: bool = False
```
Deviation: plain `bool` (not `StrictBool`) to match `dry_run`/`run_test`; non-bool guard lives in the wrapper.

**3. `cleanup_on_failure` parameter on `orchestrate_deploy_action` (signature lines 1952-1969).** Add `cleanup_on_failure: bool = False`; thread into `OrchestrateDeployRequest(...)` (1986-2002); re-read `cleanup_on_failure = request.cleanup_on_failure` (~2009-2016). Consulted ONLY on real-run failure paths.

**4. Extend `LogsStage` (lines 261-278).**
```python
error_code: Optional[str] = None
failed_stage: Optional[str] = None
next_step: Optional[str] = None
artifact_error_code: Optional[str] = None
artifact_failed_stage: Optional[str] = None
artifact_next_step: Optional[str] = None
```

**5. Replace placeholder `CleanupStage` (281-283), add `CleanupOperation`.**
```python
class CleanupOperation(BaseModel):
    tool: str
    action: str
    resource_type: str
    resource_id: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    reason: str
    destructive: bool = True

class CleanupStage(BaseModel):
    status: StageStatus
    cleanup_id: Optional[str] = None
    dry_run: bool = True
    mutation_allowed: bool = False
    operations: List[CleanupOperation] = Field(default_factory=list)
    results: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    next_step: Optional[str] = None
```
`StageStatus` (51-69) already has `planned`/`not_required`/`blocked`/`completed`/`failed`/`warning` — no change.

**6. Failure-metadata helpers (near `_error`, after line 340).**
- `_failed_stage_for_error_code(code) -> str`: package codes (`BOOMI_CLIENT_REQUIRED`,`PACKAGE_*`)→`"package"`; deploy (`DEPLOY_*`)→`"deployment"`; env/runtime/process attach (`ENVIRONMENT_VERIFY_FAILED`,`RUNTIME_VERIFY_FAILED`,`RUNTIME_ENV_*`,`PROCESS_ENV_*`,`PROCESS_RUNTIME_*`)→`"runtime_attachment"`; `SCHEDULE_*`→`"schedule"`; `TEST_*`→`"execution"`; `LOG_RETRIEVAL_FAILED`/`ARTIFACT_RETRIEVAL_FAILED`→`"logs"`; default `"unknown"`.
- `_next_step_for_failure(error, failed_stage, summary) -> str`: one actionable hint per stage, specialized by `error.code` where helpful.
- `_prior_stage_summary(failed_stage, package, deployment, runtime_attachment, schedule, execution=None, logs=None) -> Dict[str, Any]`: compact dict of stages that ran BEFORE the failed one, with ids where present.

**7. Extend `_stage_summary` (lines 1603-1664).** Add to `resource_reuse`: `"package": package.status=="reused"`, `"deployment": deployment.status=="reused"`. Add to `resource_changes`: `"package": package.status=="created"`, `"deployment": deployment.status=="deployed"`. Add `stage_statuses` sub-dict for package/deployment/runtime_attachment/schedule (+execution/logs when supplied). Preserve all existing keys. (Breaks `test_full_success_contract` exact-equality on `resource_reuse`/`resource_changes` — update that test.)

**8. Cleanup planning helpers (near response assembly, ~line 1716).**
- `_cleanup_operations_for_failure(package, deployment, runtime_attachment, schedule, *, environment_id, runtime_id, process_id) -> List[CleanupOperation]`: reverse-creation order, created/changed resources only:
  1. schedule delete/disable when `schedule.changed is True` (`manage_schedules`)
  2. `detach_process_atom` when `process_runtime_attachment_status=="attached"` (`manage_deployment`)
  3. `detach_process_environment` when `process_env_attachment_status=="attached"`
  4. runtime `detach` when `runtime_env_attachment_status=="attached"` (`manage_runtimes`)
  5. `undeploy` when `deployment.status=="deployed"` (`manage_deployment`)
  6. `delete_package` when `package.status=="created"` (`manage_deployment`)
  Each carries `reason` + `destructive=True`. Reused/not_required resources never listed (retry reuses them).
- `_cleanup_stage_for_failure(..., cleanup_on_failure, boomi_client, profile) -> CleanupStage`:
  - empty ops → `status="not_required"`, dry_run, helpful next_step.
  - `cleanup_on_failure False` (default) → `status="planned"`, `dry_run=True`, `mutation_allowed=False`, ops listed, NO SDK calls.
  - `cleanup_on_failure True` → execute each op via existing `_call_deployment_action`/`_call_runtime_action`/`_call_schedule_action`, record results; status `completed`/`warning`/`failed`; `CLEANUP_OPERATION_FAILED` in warnings on failure; `dry_run=False`, `mutation_allowed=True`.

**9. Modify the three failed-response builders** to attach top-level `error_code`/`failed_stage`/`prior_stage_summary`/`next_step` and override `downstream["cleanup"]` with `_cleanup_stage_for_failure(...)`:
- `_blocked_real_run_response` (1827-1856) — thread cleanup_on_failure/boomi_client/profile/environment_id/runtime_id/process_id.
- `_runtime_or_schedule_failed_response` (1859-1895).
- `_real_run_with_test_response` (1898-1946) ONLY when `success=False` (keep `not_required` on success).
Mechanism: let `_blocked_downstream_stages`/`_execution_log_cleanup_stages` keep emitting placeholders, then OVERRIDE `downstream["cleanup"]` + add the four top-level keys in each failed builder after `_assemble_response` (mirrors the `response["warnings"]` post-processing at 1943-1945). Keeps success/plan paths unchanged.

**10. Modify `_logs_stage_from_results` (1441-1515).** In `fetch_logs` failure branch (1478-1490): set `error_code=LOG_RETRIEVAL_FAILED`, `failed_stage="logs"`, monitor_platform retry `next_step`. In `fetch_artifacts` failure branch (1502-1512): `artifact_error_code=ARTIFACT_RETRIEVAL_FAILED`, `artifact_failed_stage="logs"`, artifact retry `next_step`. Do NOT flip `_success`. Mirror into `summary["test"]` (`log_error_code`/`log_next_step`/`artifact_error_code`/`artifact_next_step`).

**`orchestrate_deploy_action` wiring:** at each failure call site (client-None ~2064, package ~2095, deploy ~2123, runtime ~2151, schedule ~2182, failed-test ~2221) pass `cleanup_on_failure`, `boomi_client`, `profile`, `environment_id`, `runtime_id`, `process_id=target.process_component_id`. Client-None path → cleanup naturally `not_required`.

### `server.py`

1. `_ORCH_CONFIG_KEYS` (2844-2859): add `"cleanup_on_failure"`.
2. Wrapper bool guard (after dry_run guard 3046-3067, before step-3): if `cleanup_on_failure` present and not `bool`, return `INVALID_CONFIG_TYPE` (`field:"cleanup_on_failure"`) before any credential read.
3. Forwarding: flows via `merged`/`call_kwargs` into engine calls (3073/3081/3109); no extra plumbing.
4. Docstring (2952-2989): add `cleanup_on_failure` to allowed keys; note new top-level keys + cleanup plan in Returns.
5. `_orchestrate_next_steps` (2862-2902): in `not _success` branch, prepend `result.get("next_step")` first if present.

### `src/boomi_mcp/categories/meta_tools.py`

1. `orchestrate_deploy` capability (5651-5687): description mentions failure metadata + dry-run cleanup planning; add `cleanup_on_failure` to config keys; add `cleanup`/`error_code`/`failed_stage`/`prior_stage_summary`/`next_step` to `response_keys`.
2. Workflow text (~6291/6300): no edit (already deployment-before-schedule/test).

### `src/boomi_mcp/categories/deployment/__init__.py`
No change.

## Test plan

**Update breaking assertions in `tests/test_orchestrate_deploy_contract.py`:**
- `test_full_success_contract` (567-571): update `resource_reuse`/`resource_changes` exact-equality to include `package`/`deployment` keys. Do NOT add top-level failure keys to success envelope (`expected_keys` 509-513 unchanged).
- `cleanup.status=="blocked"` assertions (lines 748, 947, 1163): change to `"planned"` (created/changed resources) or `"not_required"` (nothing safe). Specifically: deploy-fail 748 → `planned` + `delete_package`; runtime-attach-fail 947 → `planned` (undeploy+delete_package); schedule-fail 1163 → `not_required` (all reused).

**New contract tests:**
- `test_deploy_failure_includes_failure_metadata_and_cleanup_plan`
- `test_attach_failure_includes_failure_metadata_and_partial_summary`
- `test_schedule_failure_includes_failure_metadata_and_prior_summary`
- `test_execution_timeout_includes_failure_metadata_and_prior_summary`
- `test_log_retrieval_failure_is_structured_diagnostic_with_next_step`
- `test_retry_after_partial_attachment_failure_reuses_prior_successes`
- `test_repeated_call_with_existing_resources_does_not_duplicate_package_deploy_attach_or_schedule`
- `test_cleanup_plan_dry_run_names_exact_operations_without_mutation`
- `test_cleanup_on_failure_true_executes_destructive_operations` (opt-in path)
- Extend `test_error_code_constants_match_module` (1365-1376) for the 3 new constants.

**New wrapper tests (`tests/test_orchestrate_deploy_wrapper.py`):**
- `test_cleanup_on_failure_config_forwarded_to_engine`
- `test_invalid_cleanup_on_failure_in_config_fails_closed`

**Capability:** no existing test asserts `orchestrate_deploy` `response_keys`; optional focused assertion.

**Commands:**
- `pytest tests/test_orchestrate_deploy_contract.py tests/test_orchestrate_deploy_wrapper.py -q`
- Regression: `pytest tests/test_meta_tools_list_capabilities.py tests/test_list_capabilities_wrapper.py -q` + build_integration/manage_deployment/manage_runtimes/manage_schedules/execute_process/monitor_platform suites, then full `pytest -q`.

## Deviations from architect plan
1. `cleanup_on_failure: bool` (not `StrictBool`) — matches `dry_run`/`run_test`; guard in wrapper.
2. Override `downstream["cleanup"]` in failed builders rather than re-plumbing shared placeholder factories.
3. No `meta_tools.py` workflow-text edit (wording already correct).
4. Top-level failure keys added ONLY to failed responses; success envelope key-set unchanged.
