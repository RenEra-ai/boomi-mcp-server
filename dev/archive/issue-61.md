# M3.2 Idempotent Package Creation And Deployment Stage

## Summary
- Keep `orchestrate_deploy_action` internal and do not wire it into `server.py`.
- Add `dry_run: bool = True`; default remains plan-only/no SDK calls. `dry_run=False` performs package/deploy SDK work.
- Add optional `package_version`; effective version is `package_version.strip()` when provided, otherwise the `build_id`, so retries of the same build reuse the same package.
- Reuse `manage_deployment_action` for `list_packages`, `create_package`, `list_deployments`, and `deploy`. Do not call SDK endpoints directly.

## File-By-File Plan

### `src/boomi_mcp/categories/deployment/orchestration.py`
- Update the module docstring from “plan-only” to “dry-run by default; package/deploy when `dry_run=False`”.
- Keep the relative registry import exactly as-is:
  `from .. import integration_builder`
- Add:
  `from .packages import manage_deployment_action`
- Extend `OrchestrateDeployRequest`:
  - `dry_run: bool = True`
  - `package_version: Optional[str] = None`
- Extend `StageStatus` with:
  - `created`, `deployed`, `reused`, `failed`, `blocked`
- Add structured error constants:
  - `BOOMI_CLIENT_REQUIRED`
  - `PACKAGE_LIST_FAILED`
  - `PACKAGE_CREATE_FAILED`
  - `PACKAGE_ID_MISSING`
  - `DEPLOY_LIST_FAILED`
  - `DEPLOY_AMBIGUOUS_EXISTING`
  - `DEPLOY_CREATE_FAILED`
  - `DEPLOY_ID_MISSING`
- Extend stage models:
  - `PackageStage`: add `component_id`, `component_type`, `package_version`, `warnings: List[str]`.
  - `DeploymentStage`: add `package_id`, `active`, `current_version`, `warnings: List[str]`.
- Add helpers:
  - `_effective_package_version(package_version, build_id) -> str`
  - `_call_deployment_action(...)` wrapper around `manage_deployment_action`
  - `_deployment_is_active(dep: Dict[str, Any]) -> bool`, matching `_lookup_deployment_id` truth rules.
  - `_stage_summary(...)` / response assembly helper that includes a top-level `summary`.
  - `_find_or_create_package(...)`
  - `_find_or_create_deployment(...)`
- Response additions:
  - Add top-level `summary`:
    ```python
    {
        "package_id": ...,
        "package_version": ...,
        "deployment_id": ...,
        "environment_id": ...,
        "deployment_active": ...,
        "deployment_current_version": ...,
        "stage_warnings": {
            "package": [...],
            "deployment": [...],
        },
    }
    ```
  - Keep existing top-level `package`, `deployment`, `runtime_attachment`, `schedule`, `execution`, `logs`, `cleanup`, `warnings`, and `errors`.
- Dry-run behavior:
  - If `dry_run=True`, never call `boomi_client`.
  - Return package/deployment stages as `planned`, ids as `None`, `dry_run=True`, `plan_only=True`.
- Non-dry-run behavior:
  - Validate required inputs and resolve build target before any SDK calls.
  - If `boomi_client is None`, return `BOOMI_CLIENT_REQUIRED`, package `failed`, later stages `blocked`.
  - Package failure blocks deployment/runtime/schedule/execution/log stages.
  - Deployment failure blocks runtime/schedule/execution/log stages.
  - Successful package/deploy leaves future M3.3/M3.4 stages as current placeholders (`planned`, `skipped`, or `not_required`).

### `src/boomi_mcp/categories/deployment/packages.py`
- No planned changes.
- Do not change `_lookup_deployment_id`; existing permissive behavior is needed by current package actions.
- Orchestration will use `manage_deployment_action("list_deployments")` instead of `_lookup_deployment_id` because it needs the full deployment set to detect multiple active deployments and return active/current state.

### `tests/test_orchestrate_deploy_contract.py`
- Update the file docstring from “plan-only” to dry-run/default plus real package/deploy stage coverage.
- Add `SimpleNamespace` and `ApiError` imports.
- Add mock helpers for SDK query pages, package objects, and deployment objects.
- Update existing success-contract expectations to include `summary`, `dry_run`, `package_version`, and stage warnings.
- Keep existing default dry-run/no-SDK tests, but update wording to “dry-run mode”.

## Idempotency Algorithm

### Package
1. Compute effective package version:
   - provided nonblank `package_version`, else `build_id`.
2. Call:
   `manage_deployment_action(..., action="list_packages", config_data={"component_id": process_component_id})`
3. Filter returned packages by exact `package_version`.
4. If matches exist:
   - Reuse the newest by `created_date` descending.
   - Set package status `reused`.
   - If multiple match, add a package-stage warning but do not create another duplicate.
5. If no match:
   - Call `create_package` with:
     `component_id`, `component_type="process"`, `package_version`.
   - Set package status `created`.
6. If list/create fails or selected result lacks `package_id`, return structured `PACKAGE_*` error and block later stages.

### Deployment
1. Call:
   `manage_deployment_action(..., action="list_deployments", config_data={"package_id": package_id, "environment_id": environment_id})`
2. Active means `active is True` or string `true/1/yes`.
3. If exactly one active deployment exists:
   - Reuse it.
   - Set deployment status `reused`.
   - Copy `deployment_id`, `active`, and `current_version`.
4. If more than one active deployment exists:
   - Return `DEPLOY_AMBIGUOUS_EXISTING`.
   - Do not call `deploy`.
   - Block later stages.
5. If no active deployment exists:
   - Call `deploy` with `package_id` and `environment_id`.
   - Set deployment status `deployed`.
   - Copy `deployment_id`, `active`, and `current_version` when returned.
6. If deploy fails or result lacks `deployment_id`, return structured `DEPLOY_*` error and block later stages.

## Acceptance Tests

1. `test_dry_run_package_deploy_stages_are_planned_and_no_sdk_calls`
   - Seed valid build.
   - Use `_ExplodingClient`.
   - Call with `dry_run=True`, `package_version="1.2.3"`.
   - Assert no SDK access, package/deployment `planned`, ids `None`, summary environment/version present.

2. `test_real_run_creates_package_when_no_existing_version`
   - `query_packaged_component` returns empty page.
   - `create_packaged_component` returns package `pkg-new`.
   - Deployment query/deploy mocks complete successfully.
   - Assert package status `created`, package id/version in summary, create called once.

3. `test_real_run_reuses_existing_package_for_same_component_and_version`
   - `query_packaged_component` returns matching package plus nonmatching version.
   - Assert `create_packaged_component` not called.
   - Assert package status `reused`, reused package id, package warning present.

4. `test_real_run_deploys_package_when_no_active_deployment`
   - Existing package returned.
   - `query_deployed_package` returns no deployments or only inactive deployments.
   - `create_deployed_package` returns deployment `dep-new`.
   - Assert deployment status `deployed`, deployment id/environment/active/current state in summary.

5. `test_real_run_reuses_existing_active_deployment`
   - Existing package returned.
   - Deployment query returns one active and one inactive deployment.
   - Assert `create_deployed_package` not called.
   - Assert deployment status `reused`, active/current state copied, warning present.

6. `test_real_run_deploy_api_failure_is_structured_and_blocks_downstream`
   - Existing package returned.
   - Deployment query returns empty.
   - `create_deployed_package.side_effect = ApiError(message="Boomi denied deploy", status=500)`.
   - Assert `_success=False`, error code `DEPLOY_CREATE_FAILED`, deployment `failed`, runtime/schedule/execution/logs `blocked`.

7. `test_real_run_ambiguous_existing_active_deployments_blocks_redeploy`
   - Existing package returned.
   - Deployment query returns two active deployments.
   - Assert error code `DEPLOY_AMBIGUOUS_EXISTING`.
   - Assert deploy create not called and downstream stages blocked.

8. `test_real_run_missing_process_id_from_resolver_never_calls_sdk`
   - Seed build with one process but no resolved `component_id`.
   - Call with `dry_run=False`.
   - Assert existing `BUILD_PROCESS_ID_MISSING` and `boomi_client.mock_calls == []`.

## Validation
- Run focused tests:
  `pytest tests/test_orchestrate_deploy_contract.py tests/test_lookup_deployment_id.py`
- Run the broader relevant deployment tests if time permits:
  `pytest tests/test_manage_deployment_wrapper.py tests/test_list_process_env_attachments.py`
- Implementation completion must follow repo workflow: unit tests, `boomi-qa-tester`, commit QA-clean baseline, then Codex review until zero issues.
