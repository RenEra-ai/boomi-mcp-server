# Issue #64: Register `orchestrate_deploy`

## Summary
Wire the existing `orchestrate_deploy_action` into the public top-level `server.py` MCP surface, then expose it through `list_capabilities` and workflow hints. Do not redesign the engine or add dependencies. The wrapper will be lazy about credentials: dry-runs and invalid requests do not read secrets or construct `Boomi`.

## File Changes

### `/server.py`
- Add a guarded import near the deployment imports:
  `from boomi_mcp.categories.deployment import orchestrate_deploy_action`, falling back to `None`.
- Register a new MCP tool after `manage_deployment`:
  ```python
  @mcp.tool(annotations={"readOnlyHint": False, "destructiveHint": True, "openWorldHint": True})
  @_kb_hint
  def orchestrate_deploy(
      profile: str,
      build_id: str = None,
      environment_id: str = None,
      runtime_id: str = None,
      dry_run: bool = None,
      run_test: bool = None,
      config: str = None,
  ):
  ```
- Parse `config` exactly like existing wrappers, but include structured wrapper errors:
  malformed JSON and non-object JSON return `_success: False`, `error`, `errors`, `warnings`, and `next_steps` without calling auth, secrets, SDK, or action.
- Merge config into action params. Allowed config keys:
  `build_id`, `environment_id`, `runtime_id`, `schedule_override`, `run_test`, `dry_run`, `package_version`, `test_timeout_seconds`, `test_dynamic_properties`, `test_process_properties`, `test_log_level`, `test_fetch_logs`, `test_fetch_artifacts`, `test_log_fetch_content`.
  Top-level non-`None` args override config values.
- Credential behavior:
  - Effective `dry_run` defaults to `True`.
  - `dry_run=True`: call `orchestrate_deploy_action(boomi_client=None, creds=None, ...)`; do not call `get_current_user`, `get_secret`, or `Boomi`.
  - `dry_run=False`: first call the action with `boomi_client=None` as a no-secret preflight. If it fails for anything other than `BOOMI_CLIENT_REQUIRED`, return that structured error. If it reaches `BOOMI_CLIENT_REQUIRED`, build SDK from credentials and call the action again with `boomi_client=sdk` and `creds=creds`.
- SDK construction mirrors `manage_deployment`: `account_id`, `username`, `password`, `timeout=30000`, optional `base_url`.
- Response normalization:
  - Preserve the engine response as canonical.
  - Add missing top-level aliases: `process_id` from `target.process_component_id`, `environment_id` from `summary`/deployment/request, `runtime_id` from `summary`/runtime/request.
  - Preserve existing `summary`, stage objects, `errors`, and `warnings`.
  - Add `next_steps` strings based on success/dry-run/run-test state, always describing order as package/deploy, then runtime binding, then optional schedule/test.
- Add a startup print entry for `orchestrate_deploy` under Deployment Management.
- Do not edit `src/boomi_mcp/tools.py` or `src/boomi_mcp/server.py`; `TOOL_SCOPES` is only used by the older authenticated server for credential/account tools, not top-level `server.py` MCP registration.

### `src/boomi_mcp/categories/meta_tools.py`
- Add an `orchestrate_deploy` capability entry adjacent to `manage_deployment`.
  Mark it `read_only: False`, category `Deployment & B2B`, and document:
  required top-level args, `dry_run` defaulting true, config options, response keys, and examples for dry-run and real run.
- Update `build_integration` capability text/examples so `apply` points agents to `orchestrate_deploy` after a returned `build_id`.
- Update `_INTEGRATION_APPLY` notes to say: after `apply` returns `build_id`, call `orchestrate_deploy(..., dry_run=true)` to preview package/deploy/runtime binding and optional schedule/test; use `dry_run=false` to execute.
- Update `build_integration_from_description` workflow:
  add an `orchestrate_deploy` step after `build_integration(action='verify')`.
  The step must say deployment happens before schedule/test and must not imply schedules run before deployment.
- Update fallback workflow similarly if it includes apply/verify.
- Add/update capability tests so filtering includes `orchestrate_deploy` when the workflow references it.

### `tests/test_orchestrate_deploy_wrapper.py`
Add wrapper tests with `BOOMI_LOCAL=true`, importing top-level `server.py`, patching `server.get_current_user`, `server.get_secret`, `server.Boomi`, and action/handler seams as needed.

Cover:
- MCP registration and annotations: `readOnlyHint=False`, `destructiveHint=True`, `openWorldHint=True`.
- Malformed JSON short-circuits before auth/SDK/action.
- Non-object config short-circuits before auth/SDK/action.
- Missing required `build_id`/`environment_id`/`runtime_id` returns structured errors before credentials.
- Dry-run calls action with `boomi_client=None`, does not read credentials, and returns normalized `process_id`, `environment_id`, `runtime_id`, `summary`, `errors`, `warnings`, `next_steps`.
- Real-run SDK construction uses account credentials, timeout, and optional `base_url`.
- Invalid real-run preflight short-circuits credentials when the action reports non-`BOOMI_CLIENT_REQUIRED` validation errors.
- Stage composition through the wrapper with real `orchestrate_deploy_action` and mocked low-level handlers, asserting package/deploy occur before runtime/schedule and the public response keeps the high-level summary.

### Existing Tests To Update
- `tests/test_meta_tools_list_capabilities.py`: add `orchestrate_deploy` catalog assertions and update workflow filtering test `available_tools` sets.
- `tests/test_list_capabilities_wrapper.py`: assert the live-filtered catalog includes `orchestrate_deploy` once registered and the authoring workflow survives with the new step.

## Test Plan
Run focused tests:
```bash
pytest tests/test_orchestrate_deploy_wrapper.py tests/test_meta_tools_list_capabilities.py tests/test_list_capabilities_wrapper.py tests/test_orchestrate_deploy_contract.py
```

Then run the repo-required completion workflow after implementation: unit tests, `boomi-qa-tester` live MCP `.fn()` validation until clean, commit the QA-clean baseline, then Codex review until zero issues.

## Assumptions
- No new dependencies.
- The existing orchestration engine remains unchanged unless wrapper tests expose a genuine public-shape gap.
- Live Boomi deploy/test QA remains out of issue scope; required QA should exercise the new public MCP wrapper path without performing unintended real deployment mutations.
