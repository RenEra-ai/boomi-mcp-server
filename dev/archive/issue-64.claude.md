# Implementation Plan — Issue #64: Register `orchestrate_deploy`

## Summary

Wire the existing `orchestrate_deploy_action` (already exported from `boomi_mcp.categories.deployment`) into the public top-level `server.py` as a new `@mcp.tool`, with config parsing, structured wrapper-level errors for malformed/non-object JSON, a credential-lazy real-run path (no-secret preflight → SDK build), and response normalization that adds top-level `process_id`/`environment_id`/`runtime_id` aliases plus `next_steps`. Then surface the tool in `meta_tools.py`'s `list_capabilities` catalog, the `build_integration` capability text, and the `build_integration_from_description` workflow (deployment-before-schedule order). Add a new wrapper test file and extend the two capability test files. No new dependencies; the engine in `orchestration.py` is untouched.

## Key engine response shape (verified, not guessed)

From `orchestration.py` `_assemble_response` (lines 1716–1754) the canonical engine response always contains these top-level keys: `_success`, `profile`, `build_id`, `dry_run`, `plan_only`, `integration_name`, `target`, `component_summary`, `package`, `deployment`, `runtime_attachment`, `schedule`, `execution`, `logs`, `cleanup`, `summary`, `warnings`, `errors` (and `error` only on failures). It does NOT emit top-level `process_id`/`environment_id`/`runtime_id`. The real sources for the wrapper's aliases:

- `process_id` ← `response["target"]["process_component_id"]` (always present; `ResolvedBuildTarget.process_component_id` is required). Fallback chain: `target.process_component_id` → `runtime_attachment["process_id"]` → `schedule["process_id"]`.
- `environment_id` ← `response["summary"]["environment_id"]` (set from `deployment.environment_id`, `_stage_summary` line 1621). Fallback chain: `summary["environment_id"]` → `deployment["environment_id"]` → the request's `environment_id` argument.
- `runtime_id` ← `response["summary"]["runtime_id"]` (set from `runtime_attachment.runtime_id`, `_stage_summary` line 1624). Fallback chain: `summary["runtime_id"]` → `runtime_attachment["runtime_id"]` → the request's `runtime_id` argument.

Note: on the BOOMI_CLIENT_REQUIRED preflight / blocked responses, `summary["environment_id"]` is populated but `summary["runtime_id"]` is null in the blocked downstream stages, so the request-argument fallback fills `runtime_id`. The aliases are added only when the engine returned a dict that already has the canonical envelope; they are NOT added to wrapper-level structured-error short-circuits (malformed/non-object JSON), which return their own minimal envelope.

---

## File-by-file

### 1. `/server.py`

**(a) Import guard** — insert immediately after the existing Deployment Tools guard block (after line 291, before the `# --- Execution Tools ---` comment):

```python
# --- Deployment Orchestration Tool (issue #64) ---
try:
    from boomi_mcp.categories.deployment import orchestrate_deploy_action
    print(f"[INFO] Deployment orchestration tool loaded successfully")
except ImportError as e:
    print(f"[WARNING] Failed to import deployment orchestration tool: {e}")
    orchestrate_deploy_action = None
```

Use the package-level import — already in `__init__.py`'s `__all__`. Matches the relative/namespace caveat in the orchestration module docstring (wrapper does not touch `_BUILD_REGISTRY`).

**(b) MCP tool registration** — insert a new `if orchestrate_deploy_action:` block immediately after the `manage_deployment` registration's closing `print(...)` (line 2828), before the `execute_process` section (line 2831). Mirror `manage_deployment` (2703–2828) + `execute_process` annotation style (2835):

```python
# --- Deployment Orchestration MCP Tool (issue #64) ---
if orchestrate_deploy_action:
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
        ...
```

Body logic, in order:

1. **Parse `config`** like `manage_deployment` (2786–2794) BUT return the richer structured envelope on failure (no SDK/auth/action call):
   - On `json.JSONDecodeError`/`TypeError`: `{"_success": False, "error": f"Invalid config (must be a JSON string): {e}", "errors": [{"code": "INVALID_CONFIG_JSON", "message": str(e), "field": "config"}], "warnings": [], "next_steps": [<fix-JSON hint>]}`.
   - If parsed value not a `dict`: same shape, `"error": "config must be a JSON object, not " + type(config_data).__name__`, `code: "INVALID_CONFIG_TYPE"`.
   - Must NOT call `get_current_user`/`get_secret`/`Boomi`/`orchestrate_deploy_action`.

2. **Merge config into action params.** Allowed keys only: `build_id`, `environment_id`, `runtime_id`, `schedule_override`, `run_test`, `dry_run`, `package_version`, `test_timeout_seconds`, `test_dynamic_properties`, `test_process_properties`, `test_log_level`, `test_fetch_logs`, `test_fetch_artifacts`, `test_log_fetch_content`. Top-level non-`None` args (`build_id`, `environment_id`, `runtime_id`, `dry_run`, `run_test`) override the config value. Define `_ORCH_CONFIG_KEYS` tuple near the wrapper. `merged = {k: config_data[k] for k in _ORCH_CONFIG_KEYS if k in config_data}` then apply arg overrides.

3. **Effective `dry_run` defaults to `True`**: `effective_dry_run = merged.get("dry_run", True)`; coerce nothing — engine pydantic emits structured `INVALID_REQUEST` for bad types.

4. **Dry-run path** (`effective_dry_run` truthy): `orchestrate_deploy_action(boomi_client=None, profile=profile, creds=None, **merged-without-dry_run, dry_run=True)`. No `get_current_user`/`get_secret`/`Boomi`. Normalize + return.

5. **Real-run path** (`effective_dry_run` falsy):
   - **No-secret preflight.** Call `orchestrate_deploy_action(boomi_client=None, profile=profile, creds=None, **merged-with-dry_run=False)`. If `_success` False AND error codes do NOT include `BOOMI_CLIENT_REQUIRED` → return normalized preflight result (catches required-field, build-resolution, schedule-override-invalid before any secret read). `_codes = {e.get("code") for e in result.get("errors", [])}`.
   - If preflight hit `BOOMI_CLIENT_REQUIRED` → `try/except` (mirror `manage_deployment` 2796–2826): `subject = get_current_user()`; `creds = get_secret(subject, profile)`; build `sdk_params` (`account_id`/`username`/`password`/`timeout=30000`, optional `base_url`); `sdk = Boomi(**sdk_params)`; re-call `orchestrate_deploy_action(boomi_client=sdk, profile=profile, creds=creds, **merged-with-dry_run=False)`.
   - On exception: `{"_success": False, "error": str(e), "exception_type": type(e).__name__}`.

6. **Normalize response** (`_normalize_orchestrate_response(result, env_arg, runtime_arg)`): only when result is canonical envelope (`"summary" in result` or `"target" in result`):
   - `result.setdefault("process_id", (target or {}).get("process_component_id") or (runtime_attachment or {}).get("process_id") or (schedule or {}).get("process_id"))`
   - `result.setdefault("environment_id", (summary or {}).get("environment_id") or (deployment or {}).get("environment_id") or environment_id)`
   - `result.setdefault("runtime_id", (summary or {}).get("runtime_id") or (runtime_attachment or {}).get("runtime_id") or runtime_id)`
   - `setdefault` `warnings`/`errors`.
   - Add `next_steps` (always `setdefault`).

**`next_steps` content rules** (order package/deploy → runtime binding → optional schedule/test):
- `_success` False: `["Fix the reported error(s) in 'errors', then re-run orchestrate_deploy.", "Run with dry_run=true to preview the package → deploy → runtime-binding → (optional) schedule/test plan without mutating Boomi."]`. If a required-field code present, prepend hint naming missing inputs; if `BUILD_ID_UNKNOWN`, hint `build_integration(action='apply')` first.
- `_success` True AND `plan_only` True: `["This was a dry-run plan; no Boomi resources were created.", "Re-run with dry_run=false to package → deploy → bind the runtime, then optionally apply the schedule and run a test."]`.
- `_success` True AND `plan_only` False AND effective `run_test` False: `["Deployment complete: package created/reused, deployed, and runtime bound (schedule applied if requested).", "Optionally re-run with run_test=true to execute the process and fetch log/artifact diagnostics."]`.
- `_success` True AND `plan_only` False AND `run_test` True: `["Deployment + test run complete.", "Review summary.test for the execution status and log/artifact excerpts; use monitor_platform(action='execution_records') for full run detail."]`.

**Docstring**: required inputs (`profile`, `build_id`/`environment_id`/`runtime_id`); `dry_run` default True (preview, no mutation); `config` allowed keys; response keys (`_success`, `build_id`, `process_id`, `environment_id`, `runtime_id`, `package`, `deployment`, `runtime_attachment`, `schedule`, `execution`, `logs`, `summary`, `errors`, `warnings`, `next_steps`); dry-run + real-run examples. Order steps package/deploy → runtime → optional schedule/test.

End with `print("[INFO] Deployment orchestration tool registered successfully")`.

**(c) Startup print** — in `if __name__ == "__main__":` LOCAL banner, extend Deployment Management block (4252–4256):

```python
        if orchestrate_deploy_action:
            print("  orchestrate_deploy - One-call package → deploy → bind runtime → optional schedule/test")
            print("    dry_run=true previews; dry_run=false executes. Order: package/deploy, then runtime, then schedule/test")
```

Do not edit `src/boomi_mcp/tools.py` or `src/boomi_mcp/server.py` (`TOOL_SCOPES` belongs to the older authenticated server).

### 2. `src/boomi_mcp/categories/meta_tools.py`

**(a) Catalog entry** — in `list_capabilities_action`'s `tools` dict, insert `"orchestrate_deploy"` immediately after `"manage_deployment"` (ends line 5646), inside `# === Category 3: Deployment & B2B ===`. `"category": "Deployment & B2B"`, `"read_only": False`, `"implemented": True`. Document parameters (`profile` required; `build_id`/`environment_id`/`runtime_id`; `dry_run` default true; `run_test`; `config` allowed keys), response keys, and two `examples` (dry-run preview, real run). Order package/deploy → runtime → optional schedule/test.

**(b) `build_integration` capability text** — update `"build_integration"` (5784–5799). Extend `description` + add example: after `apply` returns `build_id`, call `orchestrate_deploy(profile=..., build_id="<uuid-from-apply>", environment_id=..., runtime_id=..., dry_run=true)` to preview then `dry_run=false` to execute. Deployment before schedule/test.

**(c) `build_integration_from_description` workflow** — in `workflows` dict (6234–6254), add step after step 7 (`build_integration(action='verify')`):
```
"8. orchestrate_deploy(profile='...', build_id='<uuid-from-apply>', environment_id='<env-id>', runtime_id='<runtime-id>', dry_run=true) → preview package → deploy → runtime-bind → optional schedule/test; re-run with dry_run=false to execute (deployment happens BEFORE any schedule/test)."
```
Regex extractor (6297–6304, `re.match(r"[A-Z]*\d+\.\s+(\w+)\(", step)`) picks up `orchestrate_deploy`. Fallback (6245–6253, F1–F4) includes apply/verify → add `F5. orchestrate_deploy(..., dry_run=true) → preview/execute deploy`.

Contract-test interaction: `test_workflow_chain_runs_through_archetype_to_build_integration_plan` asserts `referenced[:3] == AUTHORING_TOOLS` and a `build_integration(action='plan')` handoff — step 8 breaks neither. `test_workflow_starts_with_list_integration_archetypes` checks step[0]. Safe.

### 3. `tests/test_orchestrate_deploy_wrapper.py` (NEW)

Mirror `tests/test_manage_deployment_wrapper.py` + `tests/test_list_capabilities_wrapper.py`: `os.environ["BOOMI_LOCAL"]="true"` before `import server`, project root on `sys.path`. Autouse fixture patching `server.get_current_user`, `server.get_secret` (→ `FAKE_CREDS` with `account_id`/`username`/`password`), `server.Boomi`. Call via `server.orchestrate_deploy(**kwargs)`. Cases:

1. `test_registered_with_annotations` — `orchestrate_deploy` registered; annotations `destructiveHint=True`/`readOnlyHint=False`.
2. `test_malformed_json_short_circuits` — `config="not-json"`: `_success` False, `"Invalid config" in error`, has `errors`/`warnings`/`next_steps`; assert `orchestrate_deploy_action`/`get_current_user`/`get_secret`/`Boomi` all `assert_not_called()`.
3. `test_non_object_config_short_circuits` — `config="[1,2,3]"`: same, error mentions "config must be a JSON object", no auth/SDK/action.
4. `test_missing_required_fields` — dry-run with only `profile`: real action; `_success` False, codes include `BUILD_ID_REQUIRED`/`ENVIRONMENT_ID_REQUIRED`/`RUNTIME_ID_REQUIRED`; `get_secret`/`Boomi` not called.
5. `test_dry_run_passes_no_client_no_creds` — mock `server.orchestrate_deploy_action`; call with ids + default dry_run; assert action got `boomi_client=None`, `creds=None`, `dry_run=True`; `get_current_user`/`get_secret`/`Boomi` not called; normalized response has top-level `process_id`/`environment_id`/`runtime_id` + `next_steps` (feed mock a realistic plan dict).
6. `test_real_run_builds_sdk` — mock action: `BOOMI_CLIENT_REQUIRED` failure on first (preflight) call, success envelope on second; `dry_run=False`; assert second call got `boomi_client=<patched Boomi return>`, `creds=FAKE_CREDS`; `get_secret`/`Boomi` called; SDK params (`account_id`/`username`/`password`/`timeout=30000`).
7. `test_real_run_invalid_input_short_circuits_before_secret` — mock action: non-`BOOMI_CLIENT_REQUIRED` failure on preflight; `dry_run=False`; `get_secret`/`Boomi` NOT called; structured failure returned.
8. `test_stage_composition_with_real_action` — patch `orchestration.manage_deployment_action`/`manage_environments_action`/`manage_runtimes_action`/`manage_schedules_action` with fakes (model on `test_orchestrate_deploy_contract.py`), seed `_BUILD_REGISTRY` with single-process build, call wrapper `dry_run=False`; assert envelope has `package`/`deployment`/`runtime_attachment`/`schedule` + wrapper `process_id`/`runtime_id`/`environment_id`/`next_steps`. Reuse `src.boomi_mcp...` single-namespace discipline; clean up seeded ids.

### 4. `tests/test_meta_tools_list_capabilities.py`

- `test_orchestrate_deploy_in_capabilities`: `tools = list_capabilities_action()["tools"]`; assert `"orchestrate_deploy" in tools`, `category == "Deployment & B2B"`, `read_only is False`.
- `test_orchestrate_deploy_filtered_out_when_not_registered`: `only={"build_integration"}`; assert `"orchestrate_deploy" not in list_capabilities_action(available_tools=only)["tools"]`.
- Update `available_tools` sets in `build_integration_from_description` workflow-preservation tests (`test_authoring_workflow_preserved_when_all_referenced_tools_present`, `test_workflow_fallback_*`) to include `"orchestrate_deploy"` so the now-8-step workflow's refs stay a subset. `test_authoring_workflow_dropped_when_tools_not_registered` (`only={"build_integration","get_schema_template","list_boomi_profiles"}`) still correctly drops — no change.

### 5. `tests/test_list_capabilities_wrapper.py`

Add `test_list_capabilities_wrapper_includes_orchestrate_deploy`: call `server.mcp.call_tool("list_capabilities", {})` via `_run_async`, parse payload, assert `"orchestrate_deploy" in payload["tools"]`. Existing `test_list_capabilities_wrapper_filters_to_live_registry` (`catalog_names <= registered_names`) still holds.

## Test plan

- `python -m pytest tests/test_orchestrate_deploy_wrapper.py -q` (new).
- `python -m pytest tests/test_meta_tools_list_capabilities.py tests/test_list_capabilities_wrapper.py -q` (updated).
- `python -m pytest tests/test_orchestrate_deploy_contract.py tests/test_manage_deployment_wrapper.py -q` (regression — must stay green).
- Per CLAUDE.md: after unit tests pass, launch `boomi-qa-tester` against `orchestrate_deploy` (live `.fn()` dry-run with a real `build_id`, plus malformed-config and missing-field cases), commit QA-clean baseline, run Codex review.

## Potential contract-breakage / under-spec resolutions

1. **Workflow filtering drops build_integration_from_description.** Adding `orchestrate_deploy` step makes the filter require it. Resolution: add `"orchestrate_deploy"` to the relevant `only` sets; leave the negative test's set unchanged.
2. **`creds=` keyword.** `orchestrate_deploy_action` accepts `creds` (line 1969), used only for run-test log/artifact download. `creds=None` on dry-run + preflight is correct.
3. **Aliases on error envelopes.** Only normalize canonical engine envelopes; JSON short-circuits build their own `errors`/`warnings`/`next_steps` and skip aliases (no null aliases on malformed input).
4. **`run_test` for `next_steps`.** Use effective merged `run_test` (config-or-arg).
5. **No build_integration server.py docstring edit.** apply→orchestrate guidance lives in `meta_tools.py`, not the server.py `build_integration` wrapper. Leave server.py wrapper untouched.

## Deviations from architect plan

- The architect names `_INTEGRATION_APPLY` notes as an edit site; no such symbol was located in read passes. The apply→orchestrate guidance is realized through the `build_integration` capability `description`/`examples` and the `build_integration_from_description` workflow steps. If an `_INTEGRATION_APPLY` constant exists elsewhere in the large meta_tools.py, update its note too; otherwise capability/workflow text carries the guidance. Sourcing clarification, not scope change.
- Otherwise: none.
