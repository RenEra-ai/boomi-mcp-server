# M3 `orchestrate_deploy` Live QA Runbook

Manual QA workflow + recorded evidence for validating the M3 deploy/test
orchestration capstone against a live Boomi account. Pairs with the local
contract/wrapper suites (`tests/test_orchestrate_deploy_contract.py`,
`tests/test_orchestrate_deploy_wrapper.py`) and the M2 build runbook
(`docs/M2_DATABASE_TO_API_SYNC_LIVE_QA.md`). Run this when you need real-Boomi
confidence that the one-call `orchestrate_deploy` chain — package → deploy →
bind runtime → (optional) schedule/test — works end to end against a fresh M2
`database_to_api_sync` build.

- **Milestone / Issue:** M3.7 / Issue #66 (Tier T3 — Live QA capstone, parent #9)
- **Depends on:** #65 (orchestration hardening), #30 (M2 live QA) — both closed.
- **Default QA profile:** `reneraai-5RO3DD` (Renera test account — the only
  profile that may be mutated). Reference-only profile `work` is read-only.
- **Scope:** build a fresh M2 build → dry-run preview → real package/deploy/bind
  → run-test execution to a terminal status with logs → one controlled failure →
  cleanup.
- **Out of scope (this runbook):** production deployment (`Cloud Prod` env /
  `Claude Prod Runtime`); reliability Try/Catch/DLQ proof (#51); M5/M6/M7.

## Core rules

1. **Profile + targets are fixed.** Mutate only `reneraai-5RO3DD`, deploy only to
   the TEST environment, and bind only the TEST runtime (IDs below). Never touch
   the production environment/runtime.
2. **Reuse connections by reference** so QA never handles plaintext secrets — the
   M2 build reuses the existing DB + REST connections by component id.
3. **Use a unique prefix** per run — `M3QA-<UTC timestamp>` (e.g.
   `M3QA-20260606T190152Z`) — with `conflict_policy='fail'` so a run can never
   silently reuse/overwrite a prior run's components.
4. **Dry-run before any mutation.** `orchestrate_deploy`'s `dry_run` **defaults to
   `true`**; the mutating call must pass `dry_run=false` explicitly.
5. **Record blocks, don't paper over them.** If a stage fails, record the
   structured error (`error_code` / `failed_stage` / `prior_stage_summary` /
   `next_step`) and the returned cleanup plan rather than marking QA complete.
6. **Clean up created resources.** Undeploy + delete the package, then delete the
   components the build created. Never delete the environment, the runtime, or the
   two reused connections.
7. **Never commit** account-specific raw XML, secrets, SQL with real
   credentials, mappings, full execution payloads, or full downloaded logs — only
   compact IDs / statuses / short log excerpts.

## Known live IDs (renera test account)

| Role | Name | ID |
| --- | --- | --- |
| Mutable TEST profile | — | `reneraai-5RO3DD` |
| TEST environment | `Sandbox` (classification TEST) | `5f76a03a-f38b-41b6-8b5f-36897fbeec66` |
| TEST runtime | `renera-local-atom` (ATOM, ONLINE) | `6bbff870-c270-43f3-a2ac-5c8893e2b379` |
| Reuse DB connection | `MS SQL Server Orders DB` (folder `#Common`) | `107aaef1-cb1e-4975-be44-69d120803864` |
| Reuse REST connection | `REST None` (folder `#Common`) | `7f7e0730-1152-4467-b912-e3a8ed12782a` |
| **Out of scope** PROD env | `Cloud Prod` (PROD) | `f65bda11-16e3-4bf6-b495-292c955ddd67` |
| **Out of scope** PROD runtime | `Claude Prod Runtime` (CLOUD) | `b1255e2d-df5a-4335-8325-1a97f95ab06b` |

The runtime must be **ONLINE** for the run-test stage to reach a terminal status.
Confirm with `manage_runtimes(profile=…, action="get", resource_id=…)` →
`status_detail == "ONLINE_RUNNING"` before the mutating run.

## Environment-enabled accounts (key behavior — issue #66)

The renera account is **environment-enabled** (the common Boomi setup: you deploy
to *Environments*, and runtimes attach to environments). Boomi binds a process to
a runtime through two attachments on such accounts:

- **runtime ↔ environment** (`EnvironmentAtomAttachment`), and
- **process ↔ environment** (`ProcessEnvironmentAttachment`).

The legacy **process ↔ atom direct** binding (`ProcessAtomAttachment`) is **not
used on environment-enabled accounts** — Boomi rejects it with:

```
This account uses environments. Please use ComponentEnvironmentAttachment
```

This live QA surfaced that `orchestrate_deploy` previously attempted the direct
process↔atom leg unconditionally and hard-failed at `runtime_attachment` on this
(and any environment-enabled) account, even though the package deployed and the
process↔environment binding succeeded. The fix (issue #66) records that leg as
**`not_required`** when Boomi signals the account uses environments, so the chain
proceeds to the test execution. The deploy is correct: the process runs on the
runtime via the environment. This is corroborated directly by the live platform —
Boomi's own API returns the message above, and the post-fix run executed the
process to `COMPLETE` (evidence below).

**Confirmed by Boomi documentation.** The Boomi developer docs mark the
`Process Atom Attachment` object as *"Legacy deployment, Deprecated… a deprecated
API and should no longer be used. Non-environment functionality was removed… all
accounts are changed to utilize Environments. Therefore, Boomi recommends that you
take advantage of the API functionality provided by the Process Environment
Attachment object instead."* (the same is stated for `Component Atom Attachment`).
So the direct process↔atom binding is deprecated platform-wide, and the
process↔environment attachment the engine already performs is the correct,
recommended binding — the fix matches Boomi's documented direction, not just this
account's behavior. Refs: Boomi *Process Atom Attachment object (Legacy
deployment, Deprecated)* and *Environment Atom Attachment object*.

## Tooling note

The unified `server.py` registers each MCP tool as a plain module-level function,
so call them directly — `server.orchestrate_deploy(...)`, not
`server.orchestrate_deploy.fn(...)`. `config` is a JSON **string** (use
`json.dumps`), `build_from_archetype` takes `parameters` as a **dict**, and
`orchestrate_deploy` takes top-level `profile/build_id/environment_id/runtime_id/
dry_run/run_test` plus a `config` JSON-string for the remaining engine inputs
(top-level args override matching `config` values).

```python
import json, os
os.environ["BOOMI_LOCAL"] = "true"  # or your live MCP wiring
import server

PROFILE = "reneraai-5RO3DD"
ENV_ID  = "5f76a03a-f38b-41b6-8b5f-36897fbeec66"
RT_ID   = "6bbff870-c270-43f3-a2ac-5c8893e2b379"
```

### `orchestrate_deploy` behavior to account for

- `dry_run` **defaults to `true`** — a dry-run makes no SDK call and reads no
  credentials; it returns a full `plan_only` preview (every stage `planned` /
  `not_required`, all created-resource ids null).
- Allowed `config` keys: `build_id, environment_id, runtime_id, schedule_override,
  run_test, dry_run, package_version, cleanup_on_failure, test_timeout_seconds,
  test_dynamic_properties, test_process_properties, test_log_level,
  test_fetch_logs, test_fetch_artifacts, test_log_fetch_content`.
- `test_timeout_seconds` defaults to `300`; the run-test poll backs off (2s→5s)
  bounded by it.
- `cleanup_on_failure` defaults to `false` → a failed real run returns a **dry-run
  cleanup plan** (named operations, nothing executed); `true` executes it.
- Response envelope: `_success, build_id, process_id, environment_id, runtime_id,
  package, deployment, runtime_attachment, schedule, execution, logs, cleanup,
  summary, errors, warnings, next_steps`. A failed real run adds `error_code,
  failed_stage, prior_stage_summary, next_step`.

## Step-by-step workflow

### 0. Confirm targets

```python
server.manage_environments(profile=PROFILE, action="list")          # Sandbox is TEST
server.manage_runtimes(profile=PROFILE, action="get", resource_id=RT_ID)  # ONLINE_RUNNING
```

### 1. Build a fresh M2 `database_to_api_sync` build

Author a reuse-by-id payload (DB + REST connections), a unique `component_prefix`,
caller-authored SQL + JSON profile + field map, and a manual trigger. Use the
exact parameter shape from `tests/patterns/test_database_to_api_sync_e2e.py`. A
zero-row SQL (`SELECT '1' AS source_a WHERE 1 = 0`) keeps the run-test self-
contained — the DB read returns no rows, so the REST send is skipped and the
process completes without an external dependency.

```python
spec = server.build_from_archetype("database_to_api_sync", PARAMS)["integration_spec"]
applied = server.build_integration(
    profile=PROFILE, action="apply",
    config=json.dumps({"integration_spec": spec, "conflict_policy": "fail", "dry_run": False}),
)
BUILD_ID = applied["build_id"]   # records created vs reused component ids in applied["results"]
```

> `build_id` lives in an **in-memory** registry, so the build and every
> `orchestrate_deploy` call must run in the **same process**.

### 2. `orchestrate_deploy` dry-run (no mutation)

```python
server.orchestrate_deploy(profile=PROFILE, build_id=BUILD_ID,
    environment_id=ENV_ID, runtime_id=RT_ID, run_test=True, dry_run=True)
# -> _success=True, plan_only=True; package/deployment/runtime_attachment/execution all "planned".
```

### 3. `orchestrate_deploy` real — package → deploy → bind (`run_test=false`)

```python
server.orchestrate_deploy(profile=PROFILE, build_id=BUILD_ID,
    environment_id=ENV_ID, runtime_id=RT_ID, run_test=False, dry_run=False)
# -> _success=True; package created, deployment deployed/active,
#    runtime_env + process_env attachments reused/attached, process_runtime "not_required".
```

### 4. `orchestrate_deploy` real — execute test (`run_test=true`)

```python
server.orchestrate_deploy(profile=PROFILE, build_id=BUILD_ID,
    environment_id=ENV_ID, runtime_id=RT_ID, run_test=True, dry_run=False,
    config=json.dumps({"test_timeout_seconds": 180}))
# Reuses package/deploy/bind, then executes the process and fetches logs.
# -> execution.terminal_status="COMPLETE", logs.download_url + log_excerpts present.
```

### 5. Controlled failure (no resources created)

```python
server.orchestrate_deploy(profile=PROFILE, build_id="00000000-0000-0000-0000-000000000000",
    environment_id=ENV_ID, runtime_id=RT_ID, run_test=True, dry_run=False)
# -> _success=False, errors[0].code="BUILD_ID_UNKNOWN"; nothing created, structured next_steps.
```

### 6. Cleanup

Drive cleanup from the `cleanup` plan a failed run returns, then for a successful
run undeploy + delete the package and delete the created components:

```python
server.manage_deployment(profile=PROFILE, action="undeploy", config=json.dumps({"deployment_id": DEP_ID}))
server.manage_deployment(profile=PROFILE, action="delete_package", package_id=PKG_ID)
for cid in created_component_ids:           # reverse creation order
    server.manage_component(profile=PROFILE, action="delete", component_id=cid)
```

Never delete the environment, the runtime, or the two reused connections.

## Recorded evidence — green run

```
M3 orchestrate_deploy Live QA — Evidence
----------------------------------------
Run prefix:    M3QA-20260606T190152Z
UTC window:    2026-06-06T19:01:52Z → 19:02:29Z
Profile:       reneraai-5RO3DD
Environment:   5f76a03a-f38b-41b6-8b5f-36897fbeec66 (Sandbox, TEST)
Runtime:       6bbff870-c270-43f3-a2ac-5c8893e2b379 (renera-local-atom, ONLINE_RUNNING)

Step 0  target confirm ........ PASS (Sandbox=TEST, runtime ONLINE_RUNNING)
Step 1  build_from_archetype ... PASS (_success, boomi_mutation=false, 8 component specs)
        build_integration apply (dry-run) ... PASS (dry_run=true)
        build_integration apply (real) ...... PASS
          build_id:      caa425aa-0972-40af-b844-0a914e78ce89
          created:       6 components (DB read profile, DB Get op, Target Profile,
                         REST Send op, Field Map, main process) + 2 reused connections
Step 2  orchestrate_deploy dry-run ... PASS (_success, plan_only=true; all stages "planned")
Step 3  orchestrate_deploy real (run_test=false) ... PASS (_success=true)
          package:       created   3acd5ef7-1d60-4c40-b85b-fbf5f6b3d20a (version caa425aa…)
          deployment:    deployed  577afecb-3a51-405d-a951-70c39632daf8 (active, v1)
          runtime↔env:   reused;  process↔env: reused;  process↔atom: not_required
          schedule:      not_required   (manual trigger, no schedule_override)
Step 4  orchestrate_deploy real (run_test=true) ... PASS (_success=true)
          execution_id:  execution-8e811200-fb2f-44f0-a6d9-ca4211b67c0c-2026.06.06
          request_id:    executionrecord-964f6453-432e-437c-ae6e-39052244e659
          terminal:      COMPLETE   (poll_status COMPLETED, 14.1s, 7 polls)
          documents:     inbound=1, outbound=0, inbound_error=0
          logs:          status "retrieved", downloaded=true
          download_url:  https://platform.boomi.com/account/reneraai-5RO3DD/api/download/ProcessLog-…
Step 5  controlled failure (invalid build_id) ... PASS
          _success=false, errors[0].code = BUILD_ID_UNKNOWN, no resources created
Step 6  cleanup ... PASS (undeploy + delete_package + 6 component deletes all _success;
          env / runtime / reused connections untouched)
```

Run-test log excerpt (Step 4) — the process ran on the runtime and completed:

```
INFO  initializing...   Executing Process M3QA-20260606T190152Z DB to API Sync
INFO  Start             1 document(s) found for processing.
INFO  Connector  MS SQL Server Orders DB: database Connector; … DB Get  Executing Connector Shape with 1 document(s).
INFO  Connector  MS SQL Server Orders DB: database Connector; … DB Get  No documents found. Skipping execution for the Map step.
INFO  cleanup...        Process execution completed normally.
```

**Observed log-availability lag (log-unavailable scenario).** When the execution
polls to a terminal status faster than Boomi publishes the process log, the `logs`
stage returns `status="unavailable"` (with `error_code`/`next_step`) instead of
`retrieved`. This is **diagnostic-only by design** — it never flips orchestration
`_success` to `false` — and a moments-later re-run returns `retrieved` with a
`download_url`. This was seen during QA on a sub-5s execution and is the AC's
"log-unavailable" variant in practice; it is transient Boomi timing, not a code
defect.

## Acceptance criteria mapping

- **Dry-run first, then mutation with explicit TEST env/runtime IDs** — Steps 2 → 3/4.
- **Successful run returns package ID, deployment ID, runtime attachment/schedule
  summary, execution request/execution ID, terminal status, and logs** — Step 4
  evidence (package `3acd5ef7…`, deployment `577afecb…`, attachment summary,
  `executionrecord-964f6453…` / `execution-8e811200…`, `COMPLETE`, log
  `download_url` + excerpt).
- **At least one controlled failure/timeout/log-unavailable scenario exercised or
  documented with a concrete blocker** — Step 5 (`BUILD_ID_UNKNOWN`, no resources),
  plus the environment-account attachment blocker found and fixed under #66.
- **QA note lists exact env/runtime IDs, component/build IDs, tool calls, terminal
  statuses, cleanup, and follow-up issues** — this runbook + the evidence/cleanup
  blocks above and the follow-ups below.

## Follow-up issues

- **M5/M6 (logs/observability):** the run-test log stage returns a `download_url`
  and bounded excerpts; full log download/parse hardening belongs to later
  milestones.
- **#51 (reliability):** Try/Catch/DLQ proof remains out of scope — do not attempt
  it from this runbook.
- **Schedule path:** this run used a manual trigger (`schedule` → `not_required`).
  A scheduled-trigger + `schedule_override` live pass is a worthwhile follow-up.

## Safety reminders

- Connections are reused by reference; QA never authors plaintext credentials.
- The `work` profile is read-only reference and must not be mutated.
- Keep this runbook free of raw XML, secrets, SQL credentials, mappings, full
  payloads, or full downloaded logs — IDs / statuses / short excerpts only.
