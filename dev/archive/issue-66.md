# Issue #66 Plan: M3 Orchestrate Deploy Live QA Runbook

## Summary

Create one documentation deliverable: `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md`.

This is a live QA runbook and evidence template for proving `orchestrate_deploy` end to end against a fresh M2 `database_to_api_sync` build in the mutable Boomi test account. No code changes, dependencies, tests, or repo mutations are part of the implementation plan beyond adding the new doc.

The runbook will mirror `docs/M2_DATABASE_TO_API_SYNC_LIVE_QA.md` conventions: dry-run first, reuse existing shared connections by ID, unique run prefix, explicit record-blocks rule, compact evidence capture, safety reminders, and cleanup requirements.

## File Plan

### Add `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md`

Proposed structure and content:

```markdown
# M3 Orchestrate Deploy Live QA Runbook

Issue: #66  
Milestone: M3 Deploy and Test Orchestration  
Tier: T3 - Live QA capstone  
Parent: #9  
Depends on: #65, #30

## Purpose

Validate the one-call `orchestrate_deploy` chain against a real M2 `database_to_api_sync` build in the controlled Boomi TEST account.

The live smoke must prove:

- `build_from_archetype` creates a valid M2 archetype build plan.
- `build_integration apply` dry-run previews the component creation.
- `build_integration apply` mutation creates a real build and returns a `build_id`.
- `orchestrate_deploy` dry-run previews package, deployment, runtime attachment, optional schedule handling, and test execution.
- `orchestrate_deploy` mutation packages, deploys, attaches runtime, optionally handles schedule, starts test execution, reaches terminal execution status, and returns logs or log download pointers.
- One controlled failure, timeout, or log-unavailable scenario is exercised or documented with a concrete blocker.
- All created resources are cleaned up or explicitly listed for follow-up cleanup.

## Safety Rules

- Use only profile `reneraai-5RO3DD`.
- Do not use profile `work`; it is read-only/reference only.
- Use only TEST environment `5f76a03a-f38b-41b6-8b5f-36897fbeec66`.
- Use only TEST runtime `6bbff870-c270-43f3-a2ac-5c8893e2b379`.
- Do not touch production environment `f65bda11-16e3-4bf6-b495-292c955ddd67`.
- Do not touch production runtime `b1255e2d-df5a-4335-8325-1a97f95ab06b`.
- Reuse existing shared connections by ID. Do not create or edit connection secrets.
- Do not commit raw account XML, plaintext secrets, SQL credentials, mappings containing sensitive payloads, execution payloads, or full downloaded logs.
- Record blockers exactly. Do not paper over partial failures.

## Known Live IDs

Profile:

- Mutable TEST profile: `reneraai-5RO3DD`
- Reference-only profile: `work`

TEST deployment target:

- Environment name: `Sandbox`
- Environment ID: `5f76a03a-f38b-41b6-8b5f-36897fbeec66`
- Environment classification: `TEST`
- Runtime name: `renera-local-atom`
- Runtime ID: `6bbff870-c270-43f3-a2ac-5c8893e2b379`
- Runtime state at planning time: `ONLINE` / `ONLINE_RUNNING`

Out-of-scope PROD target:

- Environment name: `Cloud Prod`
- Environment ID: `f65bda11-16e3-4bf6-b495-292c955ddd67`
- Runtime name: `Claude Prod Runtime`
- Runtime ID: `b1255e2d-df5a-4335-8325-1a97f95ab06b`

Reusable shared connections:

- Database connection: `107aaef1-cb1e-4975-be44-69d120803864`
- Database connection label: `MS SQL Server Orders DB`
- REST connection: `7f7e0730-1152-4467-b912-e3a8ed12782a`
- REST connection label: `REST None`

## Unique Run Prefix

Use one UTC timestamped prefix for every created resource in a run:

```text
M3QA-YYYYMMDD-HHMMSS
```

Example:

```text
M3QA-20260606-143000
```

Record the actual prefix in the evidence block before running any mutating call.

## Tool Behavior Notes

`orchestrate_deploy` wrapper behavior to account for:

- Top-level `build_id`, `environment_id`, `runtime_id`, `dry_run`, and `run_test` arguments override config values.
- `dry_run` defaults to `true`; the mutation call must explicitly pass `dry_run=false`.
- Expected normalized response areas include package, deployment, runtime attachment, schedule, execution, logs, cleanup, summary, `failed_stage`, and `prior_stage_summary`.
- The engine path is package → deploy → bind runtime → optional schedule handling → optional test execution → terminal status/log fetch.
- On failure, capture cleanup instructions and any already-created resource IDs before attempting teardown.

## Live QA Sequence

### Step 0 - Confirm Profile and Target State

Tool calls:

```python
list_boomi_profiles.fn({})
manage_environments.fn({
  "profile": "reneraai-5RO3DD",
  "action": "list"
})
manage_deployment.fn({
  "profile": "reneraai-5RO3DD",
  "action": "list"
})
```

Record:

- Profile list includes `reneraai-5RO3DD`.
- Sandbox environment exists and is TEST.
- Runtime `6bbff870-c270-43f3-a2ac-5c8893e2b379` is attached to Sandbox and online.

Acceptance coverage:

- Confirms controlled test account, environment, and runtime.

### Step 1 - Build M2 Archetype Plan

Tool call shape:

```python
build_from_archetype.fn({
  "profile": "reneraai-5RO3DD",
  "archetype": "database_to_api_sync",
  "name": "<RUN_PREFIX> database_to_api_sync",
  "folder_name": "<controlled TEST folder>",
  "database_connection_id": "107aaef1-cb1e-4975-be44-69d120803864",
  "rest_connection_id": "7f7e0730-1152-4467-b912-e3a8ed12782a"
})
```

Use the exact argument names and archetype parameter shape from `tests/patterns/test_database_to_api_sync_e2e.py` when executing.

Record:

- Archetype output summary.
- Planned component names.
- Any generated plan/build correlation ID returned by the tool.
- Confirmation that existing connection IDs are reused.

Acceptance coverage:

- Creates the M2 source needed for the M3 capstone.
- Avoids plaintext secrets and new connection creation.

### Step 2 - `build_integration apply` Dry-Run

Tool call shape:

```python
build_integration.fn({
  "profile": "reneraai-5RO3DD",
  "action": "apply",
  "dry_run": true,
  "...": "<archetype output from Step 1>"
})
```

Record:

- Dry-run success/failure.
- Components that would be created.
- Any validation warnings.

Acceptance coverage:

- Satisfies dry-run-first requirement before mutation.

### Step 3 - `build_integration apply` Mutation

Tool call shape:

```python
build_integration.fn({
  "profile": "reneraai-5RO3DD",
  "action": "apply",
  "dry_run": false,
  "...": "<same reviewed archetype output from Step 1>"
})
```

Record:

- `build_id`
- Created component IDs
- Created component names
- Folder ID/name
- Any warnings or partial cleanup instructions

Acceptance coverage:

- Produces the real M2 `build_id` required by `orchestrate_deploy`.

### Step 4 - `orchestrate_deploy` Dry-Run Preview

Tool call:

```python
orchestrate_deploy.fn({
  "profile": "reneraai-5RO3DD",
  "build_id": "<BUILD_ID>",
  "environment_id": "5f76a03a-f38b-41b6-8b5f-36897fbeec66",
  "runtime_id": "6bbff870-c270-43f3-a2ac-5c8893e2b379",
  "run_test": true,
  "dry_run": true
})
```

Record:

- Package preview.
- Deployment preview.
- Runtime attachment preview.
- Schedule summary, including whether schedule is absent, skipped, created, or unchanged.
- Test execution preview.
- `next_steps`.

Acceptance coverage:

- Explicitly covers `orchestrate_deploy` dry-run before mutation.
- Confirms top-level environment/runtime overrides are used.

### Step 5 - `orchestrate_deploy` Mutation With Test Run

Tool call:

```python
orchestrate_deploy.fn({
  "profile": "reneraai-5RO3DD",
  "build_id": "<BUILD_ID>",
  "environment_id": "5f76a03a-f38b-41b6-8b5f-36897fbeec66",
  "runtime_id": "6bbff870-c270-43f3-a2ac-5c8893e2b379",
  "run_test": true,
  "dry_run": false,
  "test_timeout_seconds": <reasonable timeout>
})
```

Use the timeout key only if supported by the wrapper/engine interface confirmed from `server.py` and `boomi_mcp/categories/deployment.py`; otherwise omit it and rely on the default.

Record:

- Package ID.
- Deployment ID.
- Runtime attachment result.
- Schedule summary.
- Execution request ID.
- Execution ID.
- Terminal status.
- Log excerpts, log summary, or log download pointers.
- Cleanup block returned by the tool.
- `failed_stage`, if present.
- `prior_stage_summary`, if present.
- Any warnings.

Acceptance coverage:

- Proves the full package → deploy → runtime bind → schedule handling → test execution → terminal status → logs path.
- Captures every required successful-run artifact.

### Step 6 - Controlled Failure Scenario

Use the lowest-risk failure that does not create resources:

```python
orchestrate_deploy.fn({
  "profile": "reneraai-5RO3DD",
  "build_id": "<BUILD_ID>",
  "environment_id": "5f76a03a-f38b-41b6-8b5f-36897fbeec66",
  "runtime_id": "00000000-0000-0000-0000-000000000000",
  "run_test": true,
  "dry_run": false
})
```

Expected behavior:

- Fails at runtime attachment or validation.
- Does not touch production.
- Returns a concrete error, `failed_stage`, prior-stage summary, and cleanup guidance if any package/deployment was created before failure.

If this path would create an extra package/deployment before failing, prefer this safer validation-only scenario instead:

```python
orchestrate_deploy.fn({
  "profile": "reneraai-5RO3DD",
  "build_id": "00000000-0000-0000-0000-000000000000",
  "environment_id": "5f76a03a-f38b-41b6-8b5f-36897fbeec66",
  "runtime_id": "6bbff870-c270-43f3-a2ac-5c8893e2b379",
  "run_test": true,
  "dry_run": false
})
```

Record:

- Exact failure input.
- Error message.
- Failed stage.
- Whether any resource was created.
- Cleanup performed or why no cleanup was needed.

Acceptance coverage:

- Covers controlled failure/timeout/log-unavailable scenario with concrete evidence.

### Step 7 - Cleanup

Clean up only resources created by this run.

Required cleanup targets:

- Deployment created by Step 5.
- Package created by Step 5.
- Components created by Step 3.
- Any package/deployment accidentally created by Step 6.
- Any schedule created by Step 5 if `orchestrate_deploy` created one.

Do not delete or modify:

- Environment `5f76a03a-f38b-41b6-8b5f-36897fbeec66`
- Runtime `6bbff870-c270-43f3-a2ac-5c8893e2b379`
- DB connection `107aaef1-cb1e-4975-be44-69d120803864`
- REST connection `7f7e0730-1152-4467-b912-e3a8ed12782a`
- Any production resource

Cleanup tool calls should follow the cleanup instructions returned by `orchestrate_deploy` first. If manual cleanup is required, use the appropriate existing deployment/component management tools in delete/undeploy mode and record each call.

Record:

- Resource type.
- Resource ID.
- Cleanup action.
- Cleanup result.
- Any remaining manual cleanup needed.

Acceptance coverage:

- Satisfies required cleanup of real Sandbox mutation.

## Evidence Template

Fill this section during the live run. Keep it compact and redact anything sensitive.

### Run Metadata

- Run prefix:
- UTC start:
- UTC end:
- Operator:
- Profile:
- Environment ID:
- Runtime ID:
- DB connection ID:
- REST connection ID:

### Step 0 - Target Confirmation

Tool calls:

```text
<list_boomi_profiles / manage_environments / manage_deployment call summaries>
```

Observed:

```text
<profile, environment, runtime status>
```

Result:

```text
PASS / BLOCKED
```

### Step 1 - Archetype Build Plan

Tool call summary:

```text
<redacted call summary>
```

Observed:

```text
<archetype output summary>
```

Result:

```text
PASS / FAIL / BLOCKED
```

### Step 2 - Build Apply Dry-Run

Tool call summary:

```text
<redacted call summary>
```

Observed:

```text
<dry-run summary>
```

Result:

```text
PASS / FAIL / BLOCKED
```

### Step 3 - Build Apply Mutation

Tool call summary:

```text
<redacted call summary>
```

Created resources:

```text
Build ID:
Component IDs:
Folder ID/name:
```

Result:

```text
PASS / FAIL / BLOCKED
```

### Step 4 - Orchestrate Deploy Dry-Run

Tool call summary:

```text
orchestrate_deploy build_id=<BUILD_ID> environment_id=5f76a03a-f38b-41b6-8b5f-36897fbeec66 runtime_id=6bbff870-c270-43f3-a2ac-5c8893e2b379 dry_run=true run_test=true
```

Observed:

```text
Package preview:
Deployment preview:
Runtime attachment preview:
Schedule summary:
Execution preview:
Next steps:
```

Result:

```text
PASS / FAIL / BLOCKED
```

### Step 5 - Orchestrate Deploy Mutation

Tool call summary:

```text
orchestrate_deploy build_id=<BUILD_ID> environment_id=5f76a03a-f38b-41b6-8b5f-36897fbeec66 runtime_id=6bbff870-c270-43f3-a2ac-5c8893e2b379 dry_run=false run_test=true
```

Observed:

```text
Package ID:
Deployment ID:
Runtime attachment:
Schedule summary:
Execution request ID:
Execution ID:
Terminal status:
Logs/excerpts/download pointers:
Failed stage:
Prior-stage summary:
Cleanup summary:
```

Result:

```text
PASS / FAIL / BLOCKED
```

### Step 6 - Controlled Failure

Scenario selected:

```text
Invalid runtime ID / invalid build ID / timeout / log-unavailable blocker
```

Tool call summary:

```text
<redacted call summary>
```

Observed:

```text
Error:
Failed stage:
Prior-stage summary:
Created resources, if any:
Cleanup guidance:
```

Result:

```text
PASS / FAIL / BLOCKED
```

### Step 7 - Cleanup Evidence

Cleanup performed:

```text
Resource type | Resource ID | Action | Result
```

Remaining cleanup:

```text
None / list exact resource IDs and follow-up owner
```

Result:

```text
PASS / FAIL / BLOCKED
```

## Acceptance Criteria Mapping

- Dry-run first, then mutation with explicit TEST environment/runtime IDs:
  - Covered by Steps 4 and 5.
- Successful run returns package ID, deployment ID, runtime attachment/schedule summary, execution request/execution ID, terminal status, and logs:
  - Covered by Step 5 evidence.
- Controlled failure, timeout, or log-unavailable scenario:
  - Covered by Step 6.
- QA note lists exact IDs, tool calls, terminal statuses, cleanup, and follow-up issues:
  - Covered by Evidence Template and Cleanup Evidence.
- No production deployment:
  - Covered by Safety Rules and target IDs.
- No Reliability Try/Catch/DLQ proof:
  - Explicitly out of scope for this runbook.

## Follow-Up Issues To Record If Seen

- M5 follow-up: runtime execution or process behavior defects outside deploy orchestration.
- M6 follow-up: log retrieval/download pointer gaps or observability defects.
- M7 follow-up: cleanup automation or runbook hardening gaps.
- #51 follow-up only if Try/Catch/DLQ behavior is observed as missing; do not attempt to prove it in this run.

## Commit Hygiene

The implementation commit for this task should contain only:

```text
docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md
```

Do not commit:

- Raw Boomi XML exports.
- Secrets or credentials.
- SQL payloads containing sensitive data.
- Full execution payloads.
- Full downloaded logs.
- Generated cache/build/test artifacts.
```

## Execution Notes For Implementer

- Before writing the doc, inspect `server.py` around the `orchestrate_deploy` wrapper and `boomi_mcp/categories/deployment.py` to confirm exact supported argument names such as `test_timeout_seconds`.
- Inspect `docs/M2_DATABASE_TO_API_SYNC_LIVE_QA.md` and keep the same tone and safety conventions.
- Inspect `tests/patterns/test_database_to_api_sync_e2e.py` for the exact `database_to_api_sync` archetype parameter names before running live QA.
- Keep the diff minimal: add the runbook only.
- After the live evidence is captured, fill the template in the same doc with compact redacted evidence.
- Completion still requires the repository workflow after implementation: QA agent live validation first, commit QA-clean baseline, then Codex review until zero issues.
