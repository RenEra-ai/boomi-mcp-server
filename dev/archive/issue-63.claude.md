# Issue #63 — `run_test` Stage Implementation Plan

## Summary
Add the optional `run_test` stage to the existing internal `orchestrate_deploy_action` in `src/boomi_mcp/categories/deployment/orchestration.py` only. After the schedule stage succeeds in a real run (`dry_run=False`), execute the resolved process via `execute_process_action` (forced `wait=True` polling), classify the terminal status into the `ExecutionStage`, then (on a non-failing terminal status with an `execution_id`) fetch bounded log excerpts and artifact pointers via `monitor_platform_action`. Execution/log retrieval never runs in dry-run and is blocked by any prior-stage failure. Log/artifact retrieval failure is diagnostic (`unavailable`), never an execution failure. Contract tests are added to `tests/test_orchestrate_deploy_contract.py`. No `server.py`/public MCP wiring (deferred to #64); no new dependencies.

## Key grounding facts (load-bearing)
- `execute_process_action(sdk, profile, process_id, environment_id=None, atom_id=None, config_data=None)` — process_id is positional, NOT the `manage_*` router `(sdk, profile, action, config_data)` shape. So `_call_execute_process_action` must NOT mirror `_call_schedule_action`.
- With `config_data["wait"]=True`, the action returns `request_id`, `process_id`, `environment_id`, `atom_id`, optional `execution_id`, and `execution_result` = the `_await_execution_completion` dict: `{poll_status, elapsed_seconds, poll_count, execution_id, status, process_name, atom_name, execution_time, execution_duration, inbound_document_count, outbound_document_count, inbound_error_document_count, error}`. On TIMEOUT it returns `{poll_status:"TIMEOUT", elapsed_seconds, poll_count, message}` (no `execution_id`). The action's own `_success` is `False` for ERROR/ABORTED/TIMEOUT and `True` for COMPLETE/COMPLETE_WARN. No `request_id` (missing/empty) returns `{_success:False, error:"...no request_id returned..."}` with no `request_id` key.
- `monitor_platform_action(boomi_client, profile, action, config_data, creds=None)` matches the router shape and adds `creds`. `execution_logs` returns `{_success, status_code, message, download_url, downloaded?, content?/files?, ...}`; `execution_artifacts` returns `{_success, status_code, message, download_url, ...}`. Both require `config_data["execution_id"]`.
- `creds` is needed for content download. The architect plan's "optional internal creds param not in the request model" is for this — pass it through to `monitor_platform_action`.

## File-by-file

### `src/boomi_mcp/categories/deployment/orchestration.py` (only source file)

**Imports (top, ~line 38-42):** add
```python
from ..execution import execute_process_action  # test-run execution (issue #63)
from ..monitoring import monitor_platform_action  # test-run log/artifact retrieval (issue #63)
```
Confirm the relative path: `orchestration.py` lives in `.../categories/deployment/`, so `..execution` / `..monitoring` resolve to `categories.execution` / `categories.monitoring`. (Sibling routers already use `..environments`, `..runtimes`, `..schedules` — same depth.)

**`StageStatus` (~line 44):** add literals `"completed"`, `"warning"`, `"timeout"`, `"retrieved"`, `"unavailable"`.

**Error constants (after schedule codes, ~line 97):**
```python
# Run-test stage error codes (issue #63).
TEST_EXECUTION_FAILED = "TEST_EXECUTION_FAILED"
TEST_EXECUTION_TIMEOUT = "TEST_EXECUTION_TIMEOUT"
TEST_REQUEST_ID_MISSING = "TEST_REQUEST_ID_MISSING"
```

**Module constants (near schedule constants, ~line 953):**
```python
_RUN_TEST_TIMEOUT_SECONDS = 300
_RUN_TEST_LOG_LEVEL = "ALL"
_RUN_TEST_LOG_EXCERPT_LINES = 80
_RUN_TEST_LOG_EXCERPT_CHARS = 8000
_RUN_TEST_LOG_MAX_FILES = 3
```

**`OrchestrateDeployRequest` (~line 112):** add fields after `package_version`:
```python
test_timeout_seconds: int = 300
test_dynamic_properties: Optional[Dict[str, Any]] = None
test_process_properties: Optional[Dict[str, Any]] = None
test_log_level: str = "ALL"
test_fetch_logs: bool = True
test_fetch_artifacts: bool = True
test_log_fetch_content: bool = True
```

**`ExecutionStage` (~line 218):** expand (keep `status`, `execution_id`, `run_test`):
```python
request_id: Optional[str] = None
terminal_status: Optional[str] = None       # COMPLETE / COMPLETE_WARN / ERROR / ABORTED
poll_status: Optional[str] = None           # COMPLETED / TIMEOUT
elapsed_seconds: Optional[float] = None
poll_count: Optional[int] = None
process_id: Optional[str] = None
environment_id: Optional[str] = None
atom_id: Optional[str] = None
document_counts: Optional[Dict[str, int]] = None  # inbound/outbound/inbound_error
error: Optional[str] = None
warnings: List[str] = Field(default_factory=list)
```

**`LogsStage` (~line 224):** expand (keep `status`, `log_ids`):
```python
execution_id: Optional[str] = None
log_level: Optional[str] = None
status_code: Optional[int] = None
message: Optional[str] = None
download_url: Optional[str] = None
downloaded: Optional[bool] = None
log_excerpts: Optional[List[str]] = None     # bounded, first _RUN_TEST_LOG_MAX_FILES files
artifact_status: Optional[StageStatus] = None
artifact_status_code: Optional[int] = None
artifact_message: Optional[str] = None
artifact_download_url: Optional[str] = None
error: Optional[str] = None
artifact_error: Optional[str] = None
warnings: List[str] = Field(default_factory=list)
```

**Router wrappers (after `_call_schedule_action`, ~line 536):**
```python
def _call_execute_process_action(boomi_client, profile, *, process_id, environment_id,
                                 atom_id, config_data):
    """Invoke execute_process_action (positional process_id; not the manage_* router shape)."""
    return execute_process_action(
        sdk=boomi_client, profile=profile, process_id=process_id,
        environment_id=environment_id, atom_id=atom_id, config_data=config_data,
    )

def _call_monitor_action(boomi_client, profile, action, config_data, creds=None):
    """Invoke monitor_platform_action (router shape + creds for content download)."""
    return monitor_platform_action(
        boomi_client=boomi_client, profile=profile, action=action,
        config_data=config_data, creds=creds,
    )
```

**New helpers (a dedicated `# Run-test stage helpers (issue #63)` section after `_apply_schedule_override`, ~line 1184):**

- `_build_test_execution_config(request)` → dict for `execute_process_action`'s `config_data`: force `wait=True`, set `timeout=request.test_timeout_seconds`, and pass `dynamic_properties`/`process_properties` from `test_dynamic_properties`/`test_process_properties` (only include keys when non-None so the action's `_build_*_properties` defaults apply).

- `_execution_stage_from_result(exec_result, *, run_test)` → `(ExecutionStage, Optional[OrchestrateDeployError])`. Inspect the `execute_process_action` response dict:
  - No `request_id` key (or blank) → `ExecutionStage(status="failed", run_test=True, error=...)`, error `TEST_REQUEST_ID_MISSING`.
  - `execution_result.poll_status == "TIMEOUT"` → status `"timeout"`, populate `request_id`/`poll_status`/`elapsed_seconds`/`poll_count`/`error=message`; error `TEST_EXECUTION_TIMEOUT`. No `execution_id`.
  - `poll_status == "COMPLETED"`: map `status.upper()`: `COMPLETE`→`"completed"` (no error), `COMPLETE_WARN`→`"warning"` (no error, append a warning), `ERROR`/`ABORTED`→`"failed"` + error `TEST_EXECUTION_FAILED` (message from `execution_result.error` or `"Execution ended with status: <status>"`). Always populate `request_id`, `execution_id`, `terminal_status`, `poll_status`, `elapsed_seconds`, `poll_count`, `process_id`, `environment_id`, `atom_id`, and `document_counts={inbound, outbound, inbound_error}` from the `inbound_document_count`/`outbound_document_count`/`inbound_error_document_count` fields.
  - Defensive fallback (response `_success=False` with no recognizable poll shape) → status `"failed"`, error `TEST_EXECUTION_FAILED`, message from response `error`.

- `_bounded_log_excerpts(files)` → take the first `_RUN_TEST_LOG_MAX_FILES` files (the monitoring `execution_logs` content returns either `content` (single text) or `files` (dict of name→text)); per file cap to first `_RUN_TEST_LOG_EXCERPT_LINES` lines and `_RUN_TEST_LOG_EXCERPT_CHARS` chars, prefixing each excerpt with its filename. Return `List[str]`.

- `_logs_stage_from_results(log_result, artifact_result, *, execution_id, log_level, fetch_logs, fetch_artifacts)` → `LogsStage`. Build excerpts from a successful `log_result` (use `content`/`files`); on a failing/absent `log_result` set `status="unavailable"` and `error=log_result.get("error")` (diagnostic, NOT an execution failure). Populate `status_code`, `message`, `download_url`, `downloaded` (from `_downloaded`/presence of content). For artifacts: `artifact_status`/`artifact_status_code`/`artifact_message`/`artifact_download_url`/`artifact_error`. Overall stage `status` is `"retrieved"` when logs fetched successfully, `"unavailable"` when requested but failed, `"skipped"`/`"not_required"` when not requested.

- `_run_test_stage(boomi_client, profile, request, *, target, environment_id, runtime_id, creds)` → `(ExecutionStage, LogsStage, Optional[OrchestrateDeployError])`. Orchestrates: call `_call_execute_process_action` with `_build_test_execution_config`; classify via `_execution_stage_from_result`. If execution error (failed/timeout/missing request id) → return `(exec_stage, LogsStage(status="blocked"), error)` and do NOT fetch logs (no execution_id on timeout/missing). On `completed`/`warning` with an `execution_id`: if `request.test_fetch_logs`, call `_call_monitor_action("execution_logs", {execution_id, log_level, fetch_content})`; if `request.test_fetch_artifacts`, call `_call_monitor_action("execution_artifacts", {execution_id, fetch_content})`. Build logs stage via `_logs_stage_from_results`. Log/artifact failure → logs stage `unavailable`, return error `None` (success preserved). Pass `atom_id=runtime_id`, `environment_id=environment_id` to the execute call; use `target.process_component_id` as `process_id`.

- `_real_run_with_test_response(...)` → success response that embeds the real `ExecutionStage` and `LogsStage` (parallel to `_real_run_response` but with the test stages instead of `_execution_log_cleanup_stages(...)`; `cleanup` stays `CleanupStage(status="not_required")`).

**`_stage_summary` (~line 1189):** add an optional `execution`/`logs` parameter (default `None`) and, when present, add a `summary["test"]` dict:
```python
summary["test"] = {
    "run_test": execution.run_test,
    "request_id": execution.request_id,
    "execution_id": execution.execution_id,
    "execution_status": execution.status,
    "terminal_status": execution.terminal_status,
    "poll_status": execution.poll_status,
    "elapsed_seconds": execution.elapsed_seconds,
    "poll_count": execution.poll_count,
    "document_counts": execution.document_counts,
    "execution_error": execution.error,
    "logs_status": logs.status,
    "log_download_url": logs.download_url,
    "log_excerpt_count": len(logs.log_excerpts or []),
    "artifact_download_url": logs.artifact_download_url,
    "log_error": logs.error,
    "artifact_error": logs.artifact_error,
}
```
Keep existing summary keys unchanged so all current tests pass. Only add `summary["test"]` when execution/logs are real (real-run `run_test=True`); for dry-run / `run_test=False` / blocked paths, either omit it or set a minimal `{"run_test": ...}` — match it to the new tests (the new tests will read `summary["test"]` only on real run_test=True paths). Simplest non-breaking choice: add `summary["test"]` in every path with the stage objects available, defaulting the placeholder stages.

**`orchestrate_deploy_action` signature (~line 1460):** add the seven user-facing test params after `package_version`, plus an internal `creds: Optional[Dict[str, str]] = None` (NOT in the request model — passed straight to `_run_test_stage`). Thread the new fields into `OrchestrateDeployRequest(...)` construction and re-read them off `request` after normalization (mirror lines 1502-1509).

**Handler control flow:**
- Dry-run (3a) and all blocked/failed paths (`_blocked_real_run_response`, `_runtime_or_schedule_failed_response`) are unchanged — they keep `_execution_log_cleanup_stages(run_test, blocked=...)`, so execution/logs stay `skipped`/`planned`/`blocked` and NO execute/monitor calls happen.
- Replace the final success path (3g, ~line 1690). After schedule success:
  - `if not run_test:` return existing `_real_run_response(...)` (execution `skipped`, logs `skipped`).
  - `else:` call `_run_test_stage(...)`. Then:
    - exec error (failed ERROR/ABORTED / timeout / missing request id) → `_success=False`; preserve package/deploy/runtime/schedule verbatim, embed the failed `ExecutionStage`, `LogsStage(status="blocked")`, `cleanup` `not_required`; `errors=[error]`, `error_message=error.message`.
    - `completed` or `warning` → `_success=True`; embed real execution + logs stages. `warning` adds the COMPLETE_WARN warning to `warnings`/stage warnings. Log/artifact fetch failure leaves `_success=True` with logs `unavailable`.

### `tests/test_orchestrate_deploy_contract.py` (only test file)

**New fakes/helpers** (after `_deploy_ok`, ~line 277):
- `class _FakeExecuteAction` — records `(process_id, environment_id, atom_id, config_data)` per call, returns a canned `execute_process_action`-shaped dict.
- `_patch_test_actions(monkeypatch, *, execute, monitor)` — patch `orchestration.execute_process_action` and `orchestration.monitor_platform_action`; return the two fakes. Use a `_FakeAction`-style monitor fake keyed by action (`execution_logs`/`execution_artifacts`). Compose with `_bind_success`/`_patch_all` so a full schedule-clean real run reaches the test stage.
- Exec builders: `_exec_complete(execution_id="ex-1", docs=(1,1,0))`, `_exec_warn(...)`, `_exec_failed(status="ERROR", error="boom")`, `_exec_timeout(message="Timed out after 300s ...")` — each returns the `execute_process_action` response dict shape (with `request_id`, `execution_result`, `_success` set correctly per status; timeout/`_exec_failed` set `_success=False`; failed/timeout omit `execution_id` for timeout).
- Log builders: `_logs_ok(files={"process.log": "line1\nline2"})`, `_logs_fail(error="Runtime unavailable")`, `_artifacts_ok(url=...)`.

**Tests (exact list, each assertion):**

1. `test_run_test_false_real_run_skips_execution_and_logs` — full clean real run, `run_test=False`; assert `_success`, `execution["status"]=="skipped"`, `logs["status"]=="skipped"`, and the execute/monitor fakes recorded zero calls.
2. `test_run_test_dry_run_plans_without_execution_or_log_calls` — `dry_run=True, run_test=True`, `_ExplodingClient` + patched-to-explode execute/monitor; assert `execution["status"]=="planned"`, `logs["status"]=="planned"`, zero execute/monitor calls.
3. `test_run_test_success_executes_after_schedule_and_fetches_diagnostics` — clean run + schedule + `run_test=True`, `_exec_complete`, `_logs_ok`, `_artifacts_ok`; assert `_success`, `execution["status"]=="completed"`, `execution["terminal_status"]=="COMPLETE"`, `request_id`/`execution_id` present, `document_counts` populated, `logs["status"]=="retrieved"`, `logs["log_excerpts"]` non-empty, `artifact_download_url` set, `summary["test"]["execution_status"]=="completed"`. Assert execute called once with `process_id=="CID-1"`, `atom_id=="rt-1"`, and `config_data["wait"] is True`; assert monitor `execution_logs`/`execution_artifacts` each called once with the resolved `execution_id`. Assert execution ran strictly after schedule (extend the order-log pattern, or assert exec fake called only when schedule succeeded).
4. `test_run_test_complete_warn_is_success_with_warning_summary` — `_exec_warn`; assert `_success is True`, `execution["status"]=="warning"`, `terminal_status=="COMPLETE_WARN"`, a warning surfaced in `warnings`/stage warnings, logs still fetched (`retrieved`), `summary["test"]["execution_status"]=="warning"`.
5. `test_run_test_error_and_aborted_fail_with_terminal_details` — `@pytest.mark.parametrize("status", ["ERROR","ABORTED"])` with `_exec_failed(status=...)`; assert `_success is False`, `TEST_EXECUTION_FAILED` in codes, `execution["status"]=="failed"`, `execution["terminal_status"]==status`, `execution["error"]` set, `logs["status"]=="blocked"`, no monitor calls, package/deploy/runtime/schedule preserved (not blocked).
6. `test_run_test_timeout_fails_and_does_not_fetch_logs_without_execution_id` — `_exec_timeout`; assert `_success is False`, `TEST_EXECUTION_TIMEOUT` in codes, `execution["status"]=="timeout"`, `poll_status=="TIMEOUT"`, `execution_id is None`, `logs["status"]=="blocked"`, zero monitor calls.
7. `test_run_test_no_execution_id_skips_log_fetch_but_keeps_request_id` — execute returns `_success=False`/no `request_id` → assert `TEST_REQUEST_ID_MISSING`, `execution["status"]=="failed"`; AND a second variant: completed but `execution_id` absent → `_success` stays based on terminal status, `request_id` preserved, no monitor calls (can't fetch logs without execution_id). Assert `request_id` surfaced, `logs` not `retrieved`.
8. `test_run_test_log_fetch_success_is_bounded` — `_logs_ok` with a file having >80 lines / >8000 chars; assert `logs["log_excerpts"]` truncated (≤80 lines, ≤8000 chars per excerpt), ≤3 files, `summary["test"]["log_excerpt_count"]` matches.
9. `test_run_test_log_fetch_unavailable_is_diagnostic_not_execution_failure` — `_exec_complete` + `execution_logs` returns `_success=False`; assert overall `_success is True`, `execution["status"]=="completed"`, `logs["status"]=="unavailable"`, `logs["error"]` set, no `TEST_*` error code in `errors`, `summary["test"]["logs_status"]=="unavailable"`.
10. `test_run_test_dynamic_and_process_properties_pass_through` — pass `test_dynamic_properties={"K":"V"}` and `test_process_properties={"CID":{"k":"v"}}`; capture the execute fake's `config_data` and assert both passed through verbatim plus `wait=True` and `timeout` honored (`test_timeout_seconds`).
11. `test_prior_stage_failures_block_run_test_without_execute_or_monitor` — `@pytest.mark.parametrize` over package/deploy/runtime/schedule failures (reuse existing failing-response fixtures) with `run_test=True`; assert `execution["status"]=="blocked"`, `logs["status"]=="blocked"`, and execute/monitor fakes recorded zero calls.

## Test plan
- Run the targeted suite: `python -m pytest tests/test_orchestrate_deploy_contract.py -q` (and the full suite to confirm no regression in the existing 60+ contract tests, especially `test_full_success_contract` and `test_success_with_schedule_and_run_test` which assert the `execution`/`logs` placeholder statuses on dry-run — these must remain `skipped`/`planned`/`not_required`).
- Per CLAUDE.md: after unit tests pass, the `boomi-qa-tester` agent must validate `orchestrate_deploy_action` live (run_test true/false, real execution + log retrieval), then commit the QA-clean baseline, then run the Codex review.

## Deviations from architect plan
- **`_call_execute_process_action` does NOT mirror `_call_schedule_action`.** Reason: `execute_process_action(sdk, profile, process_id, environment_id, atom_id, config_data)` has a different signature than the `manage_*` routers — `process_id` is positional and there is no `action` arg. The wrapper passes `process_id`/`environment_id`/`atom_id` explicitly.
- **`_call_monitor_action` / `_run_test_stage` thread a `creds` argument** (the architect's "optional internal creds param"). Reason: `monitor_platform_action`'s `execution_logs`/`execution_artifacts` handlers require `creds` to download and extract log/artifact content; without it `download_url` is returned but no `content`/`files`, so `_bounded_log_excerpts` would be empty. The new internal `creds` param on `orchestrate_deploy_action` carries it through.
