# Implementation Plan — Issue #66: M3 Live QA orchestrate_deploy against M2 archetype output

## Summary
Add a single documentation deliverable, `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md`, that mirrors the existing `docs/M2_DATABASE_TO_API_SYNC_LIVE_QA.md` runbook: a live QA workflow + filled evidence template proving the one-call `orchestrate_deploy` chain (build_id → package → deploy → runtime attachment → optional schedule → test execution → terminal status → log summary) against a fresh M2 `database_to_api_sync` build in the mutable Boomi TEST account. The implementer (main thread) writes the doc skeleton, runs the live QA sequence capturing real redacted evidence, fills the template, and cleans up created resources. The only repo change is this new doc (plus the committed plan artifacts under `.codex/plans/`). The architect's plan is sound; the one substantive correction is that its `.fn({...})` tool-call illustrations are wrong for this repo — the unified `server.py` registers each MCP tool as a plain module-level function, so calls must be direct (`server.orchestrate_deploy(...)`), with `config` as a JSON string.

## File-by-file

### `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md` (ADD — the only source change)
Author this new runbook, structured to mirror the M2 doc's tone, "Core rules," "Tooling note," step-by-step workflow, evidence template, and safety reminders. Section outline:

1. **Header / metadata** — Milestone M3, Tier T3 Live QA capstone, Issue #66, Parent #9, depends on #65/#30 (both closed). One-paragraph purpose: prove the full `orchestrate_deploy` chain against a real M2 build. State out-of-scope explicitly (production deployment; Try/Catch/DLQ proof per #51; M5/M6/M7).

2. **Core rules / Safety** (mirror M2 §"Core rules"):
   - Default QA profile `reneraai-5RO3DD` (full demo account — mutation authorized; still clean up for hygiene). `work` profile is read-only reference, never mutate.
   - Use only TEST env `5f76a03a-f38b-41b6-8b5f-36897fbeec66` (Sandbox) and TEST runtime `6bbff870-c270-43f3-a2ac-5c8893e2b379` (renera-local-atom, ONLINE). Never touch PROD env `f65bda11-16e3-4bf6-b495-292c955ddd67` or PROD runtime `b1255e2d-df5a-4335-8325-1a97f95ab06b`.
   - Reuse shared connections by ID: DB `107aaef1-cb1e-4975-be44-69d120803864` (`MS SQL Server Orders DB`), REST `7f7e0730-1152-4467-b912-e3a8ed12782a` (`REST None`). Never author plaintext secrets / new connections.
   - Unique run prefix `M3QA-<UTC timestamp>` with `conflict_policy='fail'`.
   - Dry-run before any mutation; `orchestrate_deploy`'s `dry_run` defaults to true, so mutation calls must explicitly pass `dry_run=False`.
   - Record blocks, don't paper over them (mirror M2 rule 5).
   - Redaction: never commit raw account XML, plaintext secrets, SQL, mappings, execution payloads, or full downloaded logs (mirror M2 safety reminders). Keep evidence compact — IDs, statuses, stage names, log *excerpts/pointers* only.

3. **Tooling note** (CORRECT the architect's `.fn` convention — see Deviations). Copy the M2 doc's exact guidance verbatim in spirit: the unified `server.py` registers each MCP tool as a plain module-level function — call `server.orchestrate_deploy(...)`, `server.build_integration(...)`, `server.build_from_archetype(...)` directly, NOT `.fn(...)`. `config` is a JSON **string** (`json.dumps`); `build_from_archetype` takes `parameters` as a **dict**; `orchestrate_deploy` takes top-level `profile/build_id/environment_id/runtime_id/dry_run/run_test` plus a `config` JSON-string carrying the remaining keys. Show the M2 boot snippet (`os.environ["BOOMI_LOCAL"]="true"; import server; PROFILE="reneraai-5RO3DD"`).

4. **Tool behavior notes** (grounded in `server.py` ~2840–3156 and `orchestration.py`):
   - Top-level args override matching `config` keys (`_ORCH_CONFIG_KEYS`: build_id, environment_id, runtime_id, schedule_override, run_test, dry_run, package_version, cleanup_on_failure, test_timeout_seconds, test_dynamic_properties, test_process_properties, test_log_level, test_fetch_logs, test_fetch_artifacts, test_log_fetch_content).
   - `dry_run` defaults to true; mutation needs `dry_run=False`.
   - `test_timeout_seconds` IS a supported config key (default 300) — confirmed in `OrchestrateDeployRequest`; the architect's "use only if supported" hedge can be stated as confirmed-supported.
   - `cleanup_on_failure` is `StrictBool` (default False → returns a dry-run cleanup PLAN; True executes destructive cleanup). Pass it via `config` only as a real JSON boolean; for QA keep it False and act on the returned plan.
   - Response envelope (confirmed via `_normalize_orchestrate_response` + stage models): `_success, build_id, process_id, environment_id, runtime_id, package, deployment, runtime_attachment, schedule, execution, logs, cleanup, summary, errors, warnings, next_steps`; a failed real run adds `error_code, failed_stage, prior_stage_summary, next_step`. Key sub-fields to capture: `package.package_id`, `deployment.deployment_id`, `runtime_attachment.{runtime_env_attachment_id, process_env_attachment_id, process_runtime_attachment_id}`, `schedule.{status, schedule_id}`, `execution.{request_id, execution_id, terminal_status, poll_status, elapsed_seconds, document_counts}`, `logs.{log_ids, log_excerpts, download_url, status, error_code}`, `cleanup.{operations, dry_run, mutation_allowed}`.

5. **Live QA Sequence** (steps 0–7), each with a direct-call shape (corrected convention) and a "Record" list. Reuse the architect's step structure but with correct calls:
   - **Step 0 — Confirm target state**: `server.list_boomi_profiles()`, `server.manage_environments(profile=PROFILE, action="list")`, `server.manage_deployment(profile=PROFILE, action="list")`. Confirm profile present, Sandbox env is TEST, runtime online.
   - **Step 1 — Build M2 archetype spec**: `server.build_from_archetype("database_to_api_sync", PARAMS)` where `PARAMS` uses the exact parameter shape from `tests/patterns/test_database_to_api_sync_e2e.py` (`naming.component_prefix="M3QA-<UTC>"`, `source.binding.mode="reuse", component_id=107aaef1-...`, `target.binding.mode="reuse", component_id=7f7e0730-...`, caller-authored SQL/REST path/schema/mappings using sentinels in the doc). Note `boomi_mutation=False`, `raw_xml_exposed=False`, grab `integration_spec`.
   - **Step 2 — build_integration apply dry-run**: `server.build_integration(profile=PROFILE, action="apply", config=json.dumps({"integration_spec": spec, "conflict_policy": "fail"}))` — confirm `dry_run is True`.
   - **Step 3 — build_integration apply mutation**: same with `"dry_run": False` in the config JSON. Record `build_id`, created vs reused component IDs/names, folder.
   - **Step 4 — orchestrate_deploy dry-run**: `server.orchestrate_deploy(profile=PROFILE, build_id=BUILD_ID, environment_id="5f76a03a-...", runtime_id="6bbff870-...", run_test=True, dry_run=True)`. Record package/deploy/runtime-attachment/schedule/execution previews + `next_steps`.
   - **Step 5 — orchestrate_deploy mutation with test run**: same with `dry_run=False`, and `config=json.dumps({"test_timeout_seconds": <e.g. 300>})`. Record package ID, deployment ID, the three attachment IDs, schedule summary, execution `request_id`/`execution_id`/`terminal_status`, log excerpts or `download_url`, `summary`, and any `failed_stage`/`cleanup` block.
   - **Step 6 — Controlled failure scenario**: prefer the lowest-risk path that creates NO resources. Recommend the validation-only invalid `build_id` (all-zeros UUID) scenario — it fails at build resolution before any package/deploy, so no cleanup is needed; capture `error_code`, `failed_stage`, `prior_stage_summary`, `next_step`. (See Risks below for why this is preferred over the invalid-runtime path.)
   - **Step 7 — Cleanup**: act on the `cleanup` plan returned by Step 5 first (it names exact tool/action/resource_id in reverse creation order). Delete/undeploy the package + deployment created in Step 5, detach the process↔runtime / process↔env attachments it created, delete any schedule it created, and delete the components created in Step 3. NEVER delete the env, the runtime, or the two reused connections. Record each cleanup call + result; list any remaining manual cleanup.

6. **Evidence Template** — mirror M2's copy-and-fill block, with sub-blocks per step (Run metadata; Step 0 target confirmation; Steps 1–3 build chain with created build/component IDs; Step 4 dry-run previews; Step 5 mutation artifacts; Step 6 failure; Step 7 cleanup table `Resource type | ID | Action | Result`). The implementer fills this in the same doc with compact, redacted real values from the live run.

7. **Acceptance Criteria Mapping** — map AC(1) dry-run-first→Steps 4/5; AC(2) successful-run artifacts→Step 5 evidence; AC(3) controlled failure→Step 6; AC(4) QA note (IDs, calls, statuses, cleanup, follow-ups)→template + cleanup table. Note out-of-scope items.

8. **Follow-up issues** — record M5 (runtime/process execution defects), M6 (log retrieval/observability gaps), M7 (cleanup automation gaps), #51 only if Try/Catch/DLQ is observed missing (do not attempt to prove it).

9. **Commit hygiene** — implementation commit contains only `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md`; no XML/secrets/SQL/payloads/full logs/cache artifacts.

### `.codex/plans/issue-66.md` (already present, committed per repo convention)
No change required; it is the architect's plan artifact and is committed alongside the doc per repo convention.

## Order of operations for the implementer (main thread)
1. Write the doc skeleton (sections 1–9 above) with the corrected direct-call convention and empty evidence template.
2. Run the live QA sequence (Steps 0–7) against profile `reneraai-5RO3DD`, capturing real IDs/statuses.
3. Fill the evidence template inline with compact, redacted results (real IDs/statuses/stage names; log *excerpts/pointers* only — no raw XML/secrets/SQL/mappings/payloads/full logs).
4. Execute cleanup using the tool-returned `cleanup` plan; record results; confirm env/runtime/connections untouched.
5. Run the repo completion gates (NOT part of the doc): boomi-qa-tester live QA agent until zero issues → commit the QA-clean baseline (short one-line message) → Codex review until zero issues, scoping re-reviews to the fix delta per CLAUDE.md.

## Test plan
- No unit tests are added or required — this is a documentation deliverable; existing suites (`tests/patterns/test_database_to_api_sync_e2e.py`, the orchestrate_deploy wrapper tests) already cover the code paths and remain green (no code touched).
- The "test" for this issue is the live QA run itself, captured as embedded evidence in the doc. Validation = the boomi-qa-tester agent re-runs the documented `orchestrate_deploy` chain via direct `server.*(...)` calls and confirms the doc's recorded artifacts (package ID, deployment ID, attachment IDs, execution/terminal status, logs/pointers, controlled-failure `failed_stage`, cleanup) match reality and that the convention/IDs in the doc are correct.
- Sanity-check before committing: `git status` shows only `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md` (plus the already-tracked `.codex/plans/issue-66.md`); grep the doc to confirm no secrets, raw XML, SQL bodies, or full logs leaked.

## Risks / decision points
- **Controlled-failure choice (AC3).** Prefer the invalid `build_id` (all-zeros UUID) validation-only path: it fails at build resolution (`BUILD_ID_UNKNOWN`) before any package/deploy, creating nothing, so it needs no cleanup and cleanly exercises the structured-error contract (`error_code`, `failed_stage`, `prior_stage_summary`, `next_step`). The architect's invalid-runtime path is riskier because package/deploy stages run before runtime verification fails, leaving a package+deployment to clean up. If a richer failure with a `prior_stage_summary` is desired, document the invalid-runtime variant as a secondary scenario only after confirming and cleaning the partial resources. Either way capture the failure evidence; if neither can be exercised live, record a concrete blocker (AC3 allows "documented with a concrete blocker").
- **If any live step blocks** (runtime offline, connection missing, build/apply error, deploy/test timeout, logs unavailable): stop, record the exact block in the evidence template, do not mark QA complete, and open/list the appropriate follow-up (M5/M6/M7). A `logs` stage `unavailable`/`LOG_RETRIEVAL_FAILED` is diagnostic-only (does not flip `_success`) and itself satisfies AC3's "log-unavailable scenario" if observed.
- **Cleanup scope.** Delete ONLY run-created resources (Step 5 package/deployment/attachments/schedule, Step 3 components, any Step 6 leftovers). Leave untouched: env `5f76a03a-...`, runtime `6bbff870-...`, DB conn `107aaef1-...`, REST conn `7f7e0730-...`, and all PROD resources. Drive cleanup from the tool-returned `cleanup.operations` (reverse creation order) before any manual delete.
- **Minimal diff.** Only the new doc is a source change; `.codex/plans/issue-66.md` is the committed plan artifact per repo convention. No new dependencies, no code edits.

## Deviations from architect plan
- **Tool-call convention corrected (required).** The architect's plan illustrates every call as `tool.fn({...})` (e.g. `orchestrate_deploy.fn({...})`). That is wrong for this repo: per the M2 doc's "Tooling note" and `server.py`, the unified server registers each MCP tool as a plain module-level function, so the doc must use direct calls — `server.orchestrate_deploy(profile=..., build_id=..., ..., config=json.dumps({...}))`, `server.build_integration(profile=..., action="apply", config=json.dumps({...}))`, `server.build_from_archetype("database_to_api_sync", PARAMS)` — with `config` as a JSON **string** and `build_from_archetype` taking `parameters` as a **dict**.
- **`test_timeout_seconds` confirmed supported.** The architect hedged ("use only if supported"). It is a real `OrchestrateDeployRequest` field (default 300) and is in `_ORCH_CONFIG_KEYS`; the doc should pass it via the `config` JSON string without the hedge.
- **Controlled-failure scenario re-prioritized.** The architect listed invalid-runtime first and the validation-only invalid-build path as a fallback. I recommend the invalid-build path as the primary scenario (no resources created, no cleanup) and the invalid-runtime path only as a documented secondary, for the cleanup-safety reason above.
- Otherwise, no deviations — the deliverable, scope, evidence template, AC mapping, safety rules, and commit hygiene follow the architect's plan.
