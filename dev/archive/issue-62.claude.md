# Implementation Plan — Issue #62: Runtime Attachment & Schedule Activation Stage

## Summary

Add the M3.3 runtime-attachment and schedule-activation stages to `orchestrate_deploy_action`, running only in real mode (`dry_run=False`) and only after package+deploy succeed. Runtime binding reuses three existing routers: `manage_environments_action` (verify env) + `manage_runtimes_action` (verify runtime, runtime↔env attachment) + `manage_deployment_action` (process↔env and process↔runtime attachments). Schedule activation reuses `manage_schedules_action` (`update`/`delete`/`enable`/`disable`). All work stays inside `orchestration.py` plus its contract test file — no router changes.

The single most important correction to the architect plan: **process↔environment and process↔runtime attachments are NOT in `runtimes.py`** — they live in `manage_deployment_action` (`packages.py`). Only the runtime↔environment (`EnvironmentAtomAttachment`) binding is in `runtimes.py`. The plan below routes each binding to its real owner.

## File-by-file

### `src/boomi_mcp/categories/deployment/orchestration.py`

**Imports (after line 36):**
```python
from ..environments import manage_environments_action
from ..runtimes import manage_runtimes_action
from .packages import manage_deployment_action  # already imported
from ..schedules import manage_schedules_action
```
Keep the existing `from .. import integration_builder` relative import (module docstring forbids changing it).

**`StageStatus` (line 38):** add `"attached"`, `"updated"`, `"enabled"`, `"disabled"`; `not_required` already present. Final tuple: `planned, skipped, not_required, created, deployed, reused, attached, updated, enabled, disabled, failed, blocked`.

**New error constants (after line 68):**
ENVIRONMENT_VERIFY_FAILED, RUNTIME_VERIFY_FAILED, RUNTIME_ENV_ATTACHMENT_LIST_FAILED, RUNTIME_ENV_ATTACHMENT_CREATE_FAILED, RUNTIME_ENV_ATTACHMENT_ID_MISSING, PROCESS_ENV_ATTACHMENT_LIST_FAILED, PROCESS_ENV_ATTACHMENT_CREATE_FAILED, PROCESS_ENV_ATTACHMENT_ID_MISSING, PROCESS_RUNTIME_ATTACHMENT_LIST_FAILED, PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED, PROCESS_RUNTIME_ATTACHMENT_ID_MISSING, SCHEDULE_OVERRIDE_INVALID, SCHEDULE_UPDATE_FAILED, SCHEDULE_DELETE_FAILED, SCHEDULE_ENABLE_FAILED, SCHEDULE_DISABLE_FAILED, SCHEDULE_ID_MISSING

**`OrchestrateDeployRequest` (line 83):** keep fields unchanged. Do NOT validate `schedule_override` content in pydantic (a non-dict still produces `INVALID_REQUEST` via the type; content validation produces `SCHEDULE_OVERRIDE_INVALID` separately in `_normalize_schedule_override`, called only in the real-run path).

**`RuntimeAttachmentStage` (line 150):** preserve `attachment_id`. Expand to:
```python
status: StageStatus
attachment_id: Optional[str] = None          # alias of runtime_env_attachment_id
runtime_id: Optional[str] = None
environment_id: Optional[str] = None
process_id: Optional[str] = None
runtime_env_attachment_id: Optional[str] = None
runtime_env_attachment_status: Optional[str] = None
process_env_attachment_id: Optional[str] = None
process_env_attachment_status: Optional[str] = None
process_runtime_attachment_id: Optional[str] = None
process_runtime_attachment_status: Optional[str] = None
reused: bool = False
changed: bool = False
warnings: List[str] = Field(default_factory=list)
```

**`ScheduleStage` (line 156):** preserve `schedule_override`. Expand to:
```python
status: StageStatus
schedule_id: Optional[str] = None
schedule_status_id: Optional[str] = None
schedule_override: Optional[Dict[str, Any]] = None
process_id: Optional[str] = None
runtime_id: Optional[str] = None
environment_id: Optional[str] = None
cron: Optional[str] = None
max_retry: Optional[int] = None
enabled: Optional[bool] = None
reused: bool = False
changed: bool = False
warnings: List[str] = Field(default_factory=list)
```

**New helper wrappers (after `_call_deployment_action`, ~line 434):** mirror its exact style:
```python
def _call_environment_action(boomi_client, profile, action, config_data) -> Dict:
    return manage_environments_action(sdk=boomi_client, profile=profile, action=action, config_data=config_data)
def _call_runtime_action(boomi_client, profile, action, config_data) -> Dict:
    return manage_runtimes_action(sdk=boomi_client, profile=profile, action=action, config_data=config_data)
def _call_schedule_action(boomi_client, profile, action, config_data) -> Dict:
    return manage_schedules_action(sdk=boomi_client, profile=profile, action=action, config_data=config_data)
```

**New `_ensure_runtime_attachment(boomi_client, profile, *, process_id, environment_id, runtime_id) -> Tuple[Optional[RuntimeAttachmentStage], Optional[OrchestrateDeployError]]`:**

1. Verify environment: `_call_environment_action(..., "get", {"resource_id": environment_id})`. `not _success` → `ENVIRONMENT_VERIFY_FAILED`.
2. Verify runtime: `_call_runtime_action(..., "get", {"resource_id": runtime_id})`. `not _success` → `RUNTIME_VERIFY_FAILED`.
3. Runtime↔env attachment (`runtimes.py`):
   - `_call_runtime_action(..., "list_attachments", {"environment_id": environment_id})` → `result["attachments"]`, each has `"id"`, `"atom_id"`, `"environment_id"`. `not _success` → `RUNTIME_ENV_ATTACHMENT_LIST_FAILED`.
   - Reuse the attachment whose `atom_id == runtime_id`; else `_call_runtime_action(..., "attach", {"resource_id": runtime_id, "environment_id": environment_id})` → id from `["attachment"]["id"]`. `not _success` → `RUNTIME_ENV_ATTACHMENT_CREATE_FAILED`; missing id → `RUNTIME_ENV_ATTACHMENT_ID_MISSING`.
4. Process↔env attachment (`manage_deployment_action`, NOT runtimes):
   - `_call_deployment_action(..., "list_process_environment_attachments", {"process_id": process_id})` → `result["attachments"]`, items have `"id"`, `"process_id"`, `"environment_id"`. Failure → `PROCESS_ENV_ATTACHMENT_LIST_FAILED`.
   - Reuse item whose `environment_id == environment_id`; else `_call_deployment_action(..., "attach_process_environment", {"process_id": process_id, "environment_id": environment_id})` → `["attachment"]["id"]`. Failure → `PROCESS_ENV_ATTACHMENT_CREATE_FAILED`; missing id → `PROCESS_ENV_ATTACHMENT_ID_MISSING`.
5. Process↔runtime attachment (`manage_deployment_action`):
   - `_call_deployment_action(..., "list_process_atom_attachments", {"process_id": process_id})` → items have `"id"`, `"process_id"`, `"atom_id"`. Failure → `PROCESS_RUNTIME_ATTACHMENT_LIST_FAILED`.
   - Reuse item whose `atom_id == runtime_id`; else `_call_deployment_action(..., "attach_process_atom", {"process_id": process_id, "atom_id": runtime_id})` → `["attachment"]["id"]`. Failure → `PROCESS_RUNTIME_ATTACHMENT_CREATE_FAILED`; missing id → `PROCESS_RUNTIME_ATTACHMENT_ID_MISSING`.
6. Aggregate: `changed = any created`; `reused = all pre-existed`. Status `"reused"` if all reused, `"attached"` if any created. Set `attachment_id = runtime_env_attachment_id`. Per-leg `*_status` = `"reused"`/`"attached"`.

**New `_normalize_schedule_override(schedule_override) -> Tuple[Optional[Dict], Optional[OrchestrateDeployError]]`:** Returns `(None, None)` for None (no mutation). Accept `{"cron": ...}` shorthand (mode=scheduled, enabled=True), `{"mode":"scheduled","cron":...,"enabled":bool,"max_retry":0..5}`, `{"mode":"manual"|"disabled"}` or `{"enabled": false}` without cron (clear/disable). Validate cron is 5 parts, `enabled` bool, `max_retry` in 0..5, no unknown keys/modes → `SCHEDULE_OVERRIDE_INVALID`. Returns normalized dict like `{"mode": "scheduled"|"disabled", "cron": ..., "max_retry": ..., "enabled": ...}`.

**New `_apply_schedule_override(boomi_client, profile, *, process_id, environment_id, runtime_id, normalized) -> Tuple[ScheduleStage, Optional[OrchestrateDeployError]]`:**
- `normalized is None` → `ScheduleStage(status="not_required", schedule_override=None, process_id=..., runtime_id=..., environment_id=...)`, no SDK calls.
- `mode in {"disabled","manual"}` → `delete` (`{"process_id": process_id, "atom_id": runtime_id}`) → failure `SCHEDULE_DELETE_FAILED`; then `disable` same ids → failure `SCHEDULE_DISABLE_FAILED`. Status `"disabled"`, `enabled=False`. `schedule_id` from delete `result["schedule"]["id"]`, `schedule_status_id` from disable `result["status"]["id"]`.
- `mode == "scheduled"` → `update` (`{"process_id":..., "atom_id": runtime_id, "cron": cron, "max_retry": max_retry}`) → `result["schedule"]["id"]`. Failure → `SCHEDULE_UPDATE_FAILED`. Then if `enabled is False`: `disable` (status `"disabled"`); else `enable` (status `"enabled"`) → failures `SCHEDULE_DISABLE_FAILED`/`SCHEDULE_ENABLE_FAILED`. `schedule_status_id` from `result["status"]["id"]`. `changed=True`. Populate `cron`, `max_retry`, `enabled`.
- After a successful mutation, missing `schedule_id` → `SCHEDULE_ID_MISSING`.

Note: `atom_id` passed to schedule actions = `runtime_id`. Schedule router keys schedules by `(atom_id, process_id)` and accepts `process_id`+`atom_id` directly (no base64 id construction in orchestration).

**`_stage_summary` (line 639):** extend signature to `_stage_summary(package, deployment, runtime_attachment=None, schedule=None)` (keep defaults so existing dry-run/plan callers still work). Add summary keys: `runtime_id`, `environment_id`, `runtime_attachment_id`, `runtime_attachment_status`, `schedule_id`, `schedule_status`, `schedule_enabled`, `resource_reuse` (per-resource reused bools), `resource_changes` (per-resource changed bools). Extend `stage_warnings` dict with `"runtime_attachment"` and `"schedule"`.

**`_placeholder_downstream_stages` (line 655):** dry-run keeps runtime `status="planned"`, schedule `planned`/`not_required`. New fields default to None.

**Control flow in `orchestrate_deploy_action` (after deploy success ~line 997):**
1. `_normalize_schedule_override(schedule_override)` *before* runtime binding (invalid override fails fast with `SCHEDULE_OVERRIDE_INVALID`, blocking runtime+schedule with zero SDK calls).
2. `_ensure_runtime_attachment(...)`. On error → success=False, runtime `failed`, schedule/execution/logs/cleanup `blocked`.
3. `_apply_schedule_override(...)`. On error → success=False, schedule `failed`, execution/logs/cleanup `blocked`, runtime stage preserved.
4. On full success → assemble response with real runtime+schedule stages, execution/logs as plan placeholders (M3.4), cleanup `not_required`.

Add an assembly path that accepts real runtime_attachment and schedule stages, plus a blocked-downstream variant that preserves a successfully-completed runtime stage while blocking schedule onward.

**Dry-run guard:** unchanged — the plan path returns before any SDK call, so dry-run + override never invokes runtime/schedule helpers.

### Other files — no changes
`packages.py`, `runtimes.py`, `environments.py`, `schedules.py`, `server.py`, `deployment/__init__.py` unchanged. Verified actions exist: runtimes `get`/`list_attachments`/`attach`; environments `get`; deployment `list_process_environment_attachments`/`attach_process_environment`/`list_process_atom_attachments`/`attach_process_atom`; schedules `update`/`delete`/`enable`/`disable`. Attachment id field is `"id"` in every list/create response.

## Test plan — `tests/test_orchestrate_deploy_contract.py`

- Extend the fake recorder: keep `_FakeDeploymentAction` for `manage_deployment_action`; add analogous fakes (or one generic recorder) for environments/runtimes/schedules, patched via `monkeypatch.setattr(orchestration, "manage_environments_action"/"manage_runtimes_action"/"manage_schedules_action", fake)`. Each `__call__` signature `(sdk=None, profile=None, action=None, config_data=None, **kwargs)`.
- Add dict factories `_att(...)`, `_sched(...)`, `_status(...)` mirroring `_pkg`/`_dep`.
- Update existing real-run tests (lines 513–706) to supply default successful runtime + schedule fakes (env/runtime get OK, attachments present/created, no override → schedule `not_required`).
- Update `test_full_success_contract` (line 387, dry-run): update `stage_warnings` assertion (line 446) to include new `runtime_attachment`/`schedule` keys and new summary keys.

New tests (acceptance mapping):
- `test_real_run_reuses_existing_runtime_bindings_before_schedule`
- `test_real_run_creates_missing_runtime_and_process_bindings`
- `test_runtime_attachment_api_failure_is_structured_and_blocks_schedule`
- `test_missing_runtime_or_environment_blocks_runtime_stage`
- `test_schedule_override_none_or_manual_has_expected_schedule_calls`
- `test_schedule_override_updates_schedule_after_runtime_binding`
- `test_schedule_override_enable_disable_status_flows`
- `test_invalid_schedule_override_returns_structured_error_without_schedule_call`
- `test_schedule_api_failure_is_structured_and_blocks_execution`
- `test_dry_run_with_schedule_override_never_calls_runtime_or_schedule_helpers`
- `test_deploy_failure_blocks_runtime_and_schedule_without_schedule_calls`

Run: `python -m pytest tests/test_orchestrate_deploy_contract.py -q`, then full suite. Then per CLAUDE.md: `boomi-qa-tester` agent live `.fn()` validation, QA-clean commit, Codex review, fix/re-review until zero issues.

## Deviations from architect plan
- **Process attachments routed to `manage_deployment_action`, not `manage_runtimes_action`** — those actions only exist in `packages.py`/`manage_deployment_action`. `manage_runtimes_action` owns only the runtime↔env (`EnvironmentAtomAttachment`) binding. `manage_deployment_action` already imported (architect's 3 new imports reduce to 2: environments + runtimes + schedules).
- **Attachment id field is `"id"`** in every response, not `atom_attachment_id`.
- **Stage field names** renamed `environment_atom_attachment_*`/`process_atom_attachment_*` → `runtime_env_*`/`process_runtime_*`; `attachment_id` kept as alias of `runtime_env_attachment_id`.
- **Schedule `atom_id` = `runtime_id`** explicit; no base64 id construction needed.
- **`test_full_success_contract` + #61 real-run tests must be updated**, not just extended.
