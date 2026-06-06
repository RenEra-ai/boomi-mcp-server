# Implementation Plan — Issue #61: M3.2 Idempotent Package Creation & Deployment Stage

## Summary
Extend the internal `orchestrate_deploy_action` (in `src/boomi_mcp/categories/deployment/orchestration.py`) with a `dry_run` flag (default `True`, preserving the issue-#60 plan-only behavior) and an optional `package_version`. When `dry_run=False`, the action performs idempotent package creation/reuse and deployment creation/reuse by delegating to `manage_deployment_action` (never the SDK directly), inspecting each returned dict's `_success`/payload. Package/deploy failures return structured error codes and block all downstream stages; runtime/schedule/execution/logs/cleanup remain placeholders for M3.3/M3.4. The tool stays internal (not wired into `server.py`). TDD: write the 8 acceptance tests first, then implement.

## Key facts grounded in the code
- `manage_deployment_action(sdk, profile, action, config_data=None, **kwargs)` merges `config_data` + kwargs, dispatches, and **swallows `ApiError`/`Exception` into `{"_success": False, "error": ..., "exception_type": ...}`** (packages.py lines 968–981). So orchestration must inspect the returned dict, never try/except.
- Handler payload shapes (confirmed):
  - `list_packages` → `{"_success": True, "packages": [ {package_id, component_id, component_type, package_version, created_date, ...}, ... ], "total_count"}`
  - `create_package` → `{"_success": True, "package": {package_id, ...}, "hint": ...}`
  - `list_deployments` → `{"_success": True, "deployments": [ {deployment_id, package_id, environment_id, active(bool), current_version, version, deployed_date, ...}, ... ], "total_count"}`. Note: `_deployment_to_dict` already coerces string `active` → bool (packages.py lines 139–140), but orchestration must still tolerate raw strings to match `_lookup_deployment_id` truth rules.
  - `deploy` → `{"_success": True, "deployment": {deployment_id, active, current_version, ...}}`
- `_action_create_package` requires `component_id`, `component_type`, `package_version` (packages.py lines 234–256) — orchestration must pass `component_type="process"`.
- The first positional param of `manage_deployment_action` is `sdk`; orchestration's wrapper passes `boomi_client` as `sdk`.

---

## File-by-file

### `src/boomi_mcp/categories/deployment/orchestration.py` (main changes)

**1. Module docstring** — change the opening from "Plan-only (dry-run) deployment orchestration contract — issue #60." to describe dry-run-by-default with real package/deploy when `dry_run=False` (issue #61). Preserve the entire "REGISTRY IMPORT — READ THIS BEFORE EDITING" block verbatim.

**2. Imports** — keep `from .. import integration_builder` unchanged. Add a sibling relative import directly after it:
```python
from .packages import manage_deployment_action
```
Keep `Literal` in the typing import (StageStatus expands). No new third-party deps.

**3. `StageStatus`** — extend the Literal:
```python
StageStatus = Literal[
    "planned", "skipped", "not_required",
    "created", "deployed", "reused", "failed", "blocked",
]
```

**4. New structured-error constants** (after the existing `BUILD_*` block):
```python
BOOMI_CLIENT_REQUIRED = "BOOMI_CLIENT_REQUIRED"
PACKAGE_LIST_FAILED = "PACKAGE_LIST_FAILED"
PACKAGE_CREATE_FAILED = "PACKAGE_CREATE_FAILED"
PACKAGE_ID_MISSING = "PACKAGE_ID_MISSING"
DEPLOY_LIST_FAILED = "DEPLOY_LIST_FAILED"
DEPLOY_AMBIGUOUS_EXISTING = "DEPLOY_AMBIGUOUS_EXISTING"
DEPLOY_CREATE_FAILED = "DEPLOY_CREATE_FAILED"
DEPLOY_ID_MISSING = "DEPLOY_ID_MISSING"
```

**5. `OrchestrateDeployRequest`** — add two fields:
```python
dry_run: bool = True
package_version: Optional[str] = None
```

**6. Stage models** — extend:
```python
class PackageStage(BaseModel):
    status: StageStatus
    package_id: Optional[str] = None
    component_id: Optional[str] = None
    component_type: Optional[str] = None
    package_version: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)

class DeploymentStage(BaseModel):
    status: StageStatus
    deployment_id: Optional[str] = None
    environment_id: Optional[str] = None
    package_id: Optional[str] = None
    active: Optional[bool] = None
    current_version: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)
```
Leave RuntimeAttachment/Schedule/Execution/Logs/Cleanup stage models unchanged.

**7. New helpers** (place near existing helpers):

```python
def _effective_package_version(package_version: Optional[str], build_id: str) -> str:
    """Effective version: trimmed package_version if non-blank, else build_id."""
    if isinstance(package_version, str) and package_version.strip():
        return package_version.strip()
    return build_id
```

```python
def _call_deployment_action(
    boomi_client: Any, profile: Optional[str], action: str, config_data: Dict[str, Any],
) -> Dict[str, Any]:
    return manage_deployment_action(
        sdk=boomi_client, profile=profile, action=action, config_data=config_data,
    )
```

```python
def _deployment_is_active(dep: Dict[str, Any]) -> bool:
    """Match _lookup_deployment_id truth rules: bool True or string true/1/yes."""
    raw = dep.get("active")
    if isinstance(raw, str):
        return raw.strip().lower() in ("true", "1", "yes")
    return bool(raw) if raw is not None else False
```

```python
def _created_date_key(pkg: Dict[str, Any]) -> str:
    return pkg.get("created_date") or ""
```

`_find_or_create_package(boomi_client, profile, *, component_id, package_version) -> Tuple[Optional[PackageStage], Optional[OrchestrateDeployError]]`:
1. Call `_call_deployment_action(..., "list_packages", {"component_id": component_id})`.
2. If `not result.get("_success")`: return `(None, _error(PACKAGE_LIST_FAILED, result.get("error", ...), field="build_id", details={"component_id": component_id}))`.
3. `packages = result.get("packages") or []`. Filter to `matches = [p for p in packages if p.get("package_version") == package_version]`.
4. If `matches`: sort `matches.sort(key=_created_date_key, reverse=True)` (newest first by `created_date` desc). Select `selected = matches[0]`. `warnings = ["Multiple existing packages match version ...; reused newest by created_date."]` when `len(matches) > 1` else `[]`. status `"reused"`.
5. Else (no matches): call `create_package` via `_call_deployment_action(..., "create_package", {"component_id": component_id, "component_type": "process", "package_version": package_version})`. If `not _success`: return `(None, _error(PACKAGE_CREATE_FAILED, ...))`. `selected = result["package"]`; status `"created"`; `warnings = []`.
6. Resolve `package_id = selected.get("package_id")`. If blank: return `(None, _error(PACKAGE_ID_MISSING, ...))`.
7. Build and return `(PackageStage(status=status, package_id=package_id, component_id=component_id, component_type="process", package_version=package_version, warnings=warnings), None)`.

`_find_or_create_deployment(boomi_client, profile, *, package_id, environment_id) -> Tuple[Optional[DeploymentStage], Optional[OrchestrateDeployError]]`:
1. Call `list_deployments` with `config_data={"package_id": package_id, "environment_id": environment_id}`.
2. If `not _success`: return `(None, _error(DEPLOY_LIST_FAILED, ...))`.
3. `deployments = result.get("deployments") or []`. `active = [d for d in deployments if _deployment_is_active(d)]`.
4. If `len(active) > 1`: return `(None, _error(DEPLOY_AMBIGUOUS_EXISTING, "...", field="environment_id", details={"package_id": package_id, "environment_id": environment_id, "active_count": len(active)}))`.
5. If `len(active) == 1`: `selected = active[0]`; status `"reused"`; warnings `["Reused existing active deployment ..."]` if `len(deployments) > 1` else `[]`. Copy `deployment_id`, `active=_deployment_is_active(selected)`, `current_version=selected.get("current_version")`.
6. If `len(active) == 0`: call `deploy` with `config_data={"package_id": package_id, "environment_id": environment_id}`. If `not _success`: return `(None, _error(DEPLOY_CREATE_FAILED, ...))`. `selected = result["deployment"]`; status `"deployed"`; warnings `[]`. Copy `deployment_id`, `active`, `current_version`.
7. `deployment_id = selected.get("deployment_id")`. If blank: return `(None, _error(DEPLOY_ID_MISSING, ...))`.
8. Return `(DeploymentStage(status=status, deployment_id=deployment_id, environment_id=environment_id, package_id=package_id, active=..., current_version=..., warnings=warnings), None)`.

**8. Response assembly helper `_real_run_response`** (new, parallel to `_plan_response`): given the resolved `target`, the completed `PackageStage` + `DeploymentStage`, plus `profile/build_id/environment_id/runtime_id/schedule_override/run_test`, build a response with the SAME top-level keys as `_plan_response` PLUS a new top-level `summary`. Set `dry_run=False`, `plan_only=False`. Downstream stages (runtime_attachment, schedule, execution, logs, cleanup) keep their existing placeholder logic (these stay `planned`/`skipped`/`not_required` exactly as in `_plan_response` since M3.3/M3.4 are out of scope).

**9. Top-level `summary` block** — assembled by a helper `_stage_summary(package, deployment)`:
```python
{
  "package_id": package.package_id,
  "package_version": package.package_version,
  "deployment_id": deployment.deployment_id,
  "environment_id": deployment.environment_id,
  "deployment_active": deployment.active,
  "deployment_current_version": deployment.current_version,
  "stage_warnings": {"package": package.warnings, "deployment": deployment.warnings},
}
```
Add `summary` to BOTH the real-run response and the dry-run `_plan_response` (so the response key set is stable across dry-run and real-run). In dry-run, populate `summary` from the planned (id-less) stages: `package_id`/`deployment_id` None, `package_version` = effective version (resolvable in dry-run from `package_version`/`build_id` without any SDK call), `environment_id` set, `deployment_active`/`deployment_current_version` None, empty `stage_warnings` lists.

> Note: to keep the dry-run `summary` truthful, compute the effective package_version in `_plan_response` via `_effective_package_version(package_version, build_id)` and set the dry-run `PackageStage(status="planned", package_version=<effective>, component_id=target.process_component_id, component_type="process")`. This keeps `package_version` consistent between dry-run and real-run.

**10. Error responses for new stages** — extend `_error_response` usage. For package/deploy structured failures during a real run, the response must still carry the full stage map with `package.status="failed"` (or `deployment.status="failed"`) and downstream stages `"blocked"`, plus `errors=[...]` and `_success=False`. Add a helper `_blocked_real_run_response(...)` that builds the full response shape with the failing stage marked `failed`, all stages after it `blocked`, `summary` populated from whatever stages completed, and the structured error in `errors`. This satisfies acceptance criterion "Failed package/deploy stages return structured error codes and do not proceed to runtime/schedule/execution stages." Ordering of blocking:
- Package failure → `package="failed"`, `deployment/runtime_attachment/schedule/execution/logs/cleanup="blocked"`.
- Deploy failure → `package=<created/reused>`, `deployment="failed"`, `runtime_attachment/schedule/execution/logs/cleanup="blocked"`.

**11. `orchestrate_deploy_action` signature** — add params, preserving order and defaults:
```python
def orchestrate_deploy_action(
    boomi_client: Any = None,
    profile: Optional[str] = None,
    build_id: Optional[str] = None,
    environment_id: Optional[str] = None,
    runtime_id: Optional[str] = None,
    schedule_override: Optional[Dict[str, Any]] = None,
    run_test: bool = False,
    dry_run: bool = True,
    package_version: Optional[str] = None,
) -> Dict[str, Any]:
```
Pass `dry_run`/`package_version` into `OrchestrateDeployRequest(...)`; re-read them off the validated `request`.

**12. Control flow** (extends the existing numbered steps; ordering is load-bearing per constraints):
- Step 0: build `OrchestrateDeployRequest` (now includes `dry_run`, `package_version`).
- Step 1: required-field validation (unchanged — runs before any SDK call).
- Step 2: resolve build target (unchanged — runs before any SDK call). **This guarantees `BUILD_PROCESS_ID_MISSING` is returned with zero SDK calls even when `dry_run=False`** (acceptance test 8).
- Step 3 (NEW branch):
  - If `request.dry_run`: return `_plan_response(...)` (now including `summary`). Never touch `boomi_client`.
  - Else (real run): if `boomi_client is None`, return `_blocked_real_run_response(...)` with `BOOMI_CLIENT_REQUIRED`, `package="failed"`, downstream `"blocked"`. Otherwise:
    - `effective_version = _effective_package_version(request.package_version, build_id)`.
    - `package_stage, pkg_err = _find_or_create_package(boomi_client, profile, component_id=target.process_component_id, package_version=effective_version)`. If `pkg_err`: return blocked response (`PACKAGE_*`).
    - `deploy_stage, dep_err = _find_or_create_deployment(boomi_client, profile, package_id=package_stage.package_id, environment_id=environment_id)`. If `dep_err`: return blocked response with `package_stage` intact, `deployment="failed"`, downstream blocked (`DEPLOY_*`).
    - On success: return `_real_run_response(...)` with both stages and `summary`.

### `src/boomi_mcp/categories/deployment/packages.py`
**No changes.** `_lookup_deployment_id` is intentionally not reused (orchestration needs the full deployment set to detect multiple active deployments and surface active/current state, so it uses `list_deployments`). Confirmed `manage_deployment_action` and all four handlers already return the dict shapes orchestration depends on.

### `src/boomi_mcp/categories/deployment/__init__.py`
**No changes.** `orchestrate_deploy_action` is already exported and stays internal (not imported by `server.py`).

### `tests/test_orchestrate_deploy_contract.py`
- Update the module docstring to mention issue #61 / dry-run-by-default.
- Add imports: `from types import SimpleNamespace`; the tests mock `manage_deployment_action` at the orchestration call site, so prefer `monkeypatch.setattr(orchestration, "manage_deployment_action", fake)` over building real SDK query pages.
- Add mock helpers: `_pkg(package_id, version, created_date)` → dict; `_dep(deployment_id, active, current_version=None)` → dict; a `_FakeDeploymentAction` recorder that records `(action, config_data)` calls and returns canned dicts keyed by action.
- **Update existing success-contract expectations** (`test_full_success_contract`): add `"summary"` to `expected_keys`; assert `summary["package_version"]` equals the effective version (build_id when no override), `summary["package_id"] is None`, `summary["deployment_id"] is None`, `summary["stage_warnings"] == {"package": [], "deployment": []}`. Keep `dry_run is True`, `plan_only is True`.
- Keep all existing dry-run / no-SDK / no-mutation tests; they still pass because `dry_run` defaults to `True`.

**Add the 8 acceptance tests** (faithful to architect plan):
1. `test_dry_run_package_deploy_stages_are_planned_and_no_sdk_calls` — `_ExplodingClient`, `dry_run=True`, `package_version="1.2.3"`. Assert `_success True`, `dry_run True`, `package.status=="planned"`, `summary["package_version"]=="1.2.3"`, no SDK access.
2. `test_real_run_creates_package_when_no_existing_version` — fake list returns empty `packages`; create returns `pkg-new`; deploy list returns no active; deploy returns `dep-new`. Assert `create_package` called exactly once, `package.status=="created"`, `summary["package_id"]=="pkg-new"`.
3. `test_real_run_reuses_existing_package_for_same_component_and_version` — list returns one matching-version pkg + one non-matching; assert `create_package` NOT called, `package.status=="reused"`. Add a second matching pkg variant to assert the warning + newest-by-created_date selection.
4. `test_real_run_deploys_package_when_no_active_deployment` — existing package; `list_deployments` returns none/inactive; deploy returns `dep-new`; assert `deployment.status=="deployed"`, `summary["deployment_id"]=="dep-new"`.
5. `test_real_run_reuses_existing_active_deployment` — one active + one inactive; assert `deploy` NOT called, `deployment.status=="reused"`, warning present, `summary["deployment_active"] is True`.
6. `test_real_run_deploy_api_failure_is_structured_and_blocks_downstream` — deploy returns `{"_success": False, "error": "...500..."}`; assert `_success False`, `DEPLOY_CREATE_FAILED` in codes, `deployment.status=="failed"`, downstream stages `"blocked"`.
7. `test_real_run_ambiguous_existing_active_deployments_blocks_redeploy` — two active deployments; assert `DEPLOY_AMBIGUOUS_EXISTING`, deploy NOT called, downstream `"blocked"`.
8. `test_real_run_missing_process_id_from_resolver_never_calls_sdk` — process with no `component_id`, `dry_run=False`, `boomi_client=MagicMock()`; assert `BUILD_PROCESS_ID_MISSING` and `client.mock_calls == []` (resolution precedes any SDK call).

Add one extra test (architect-required, beyond the 8): `test_real_run_without_client_returns_boomi_client_required` — `dry_run=False`, `boomi_client=None` → `BOOMI_CLIENT_REQUIRED`, `package.status=="failed"`, downstream blocked.

---

## Test plan
1. `pytest tests/test_orchestrate_deploy_contract.py -q` — write the 8 new acceptance tests + updated contract test BEFORE implementing (TDD red), then implement until green.
2. `pytest -q` to confirm no regressions across the deployment category and dual-namespace import behavior.
3. **Stage 1 — boomi-qa-tester** agent: live `.fn()` validation of `orchestrate_deploy` covering all 8 paths. Iterate until QA reports zero issues.
4. **Stage 1.5** — commit the QA-clean baseline with a short one-line message.
5. **Stage 2 — Codex review** (`codex-companion.mjs review --wait`, scoped to the fix on re-reviews).

Edge cases to verify: effective `package_version` defaulting to `build_id` when omitted/blank; `created_date` descending sort selecting the newest matching package; active detection accepting bool `True` and string `"true"/"1"/"yes"`; downstream stages set to `"blocked"` (not `"planned"`) on any package/deploy failure; zero SDK calls when resolution fails even with `dry_run=False`.

## Deviations from architect plan
- **Test mocking**: monkeypatch `orchestration.manage_deployment_action` with a fake returning canned dicts (the wrapper already swallows `ApiError` into `{"_success": False, ...}`), rather than building real SDK query pages. Higher-signal, lower-coupling — tests orchestration's dict-inspection contract directly. `SimpleNamespace`/`ApiError` only imported if a test drives the real handler. Test-only deviation; satisfies acceptance either way.
- Added one explicit `BOOMI_CLIENT_REQUIRED` real-run test beyond the 8 enumerated; the architect's File-by-file section requires that error path but it isn't in the numbered list. No production-scope change.

All other points (internal-only tool, relative imports kept, registry read at call time, ordering of validation/resolution before SDK calls, idempotency algorithm, summary shape, blocking semantics) follow the architect plan as written.
