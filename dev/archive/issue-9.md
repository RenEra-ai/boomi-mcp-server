# Epic #9 Closeout Plan: Deploy and Test Orchestration

## Summary

No new feature code is needed. `orchestrate_deploy` is already implemented, registered, hardened, and live-QA verified by the closed child issues. The remaining work is documentation closeout for epic #9: resolve exit criterion 4 via the allowed “blocked with recorded Boomi evidence” path, reconcile stale docs, and add a closeout evidence record that makes the epic state internally consistent.

Criterion 4 should be recorded as blocked for end-to-end runtime failure-row proof, not as unverified feature absence. The evidence should distinguish:
- Verified: R1a emitter-level Try/Catch + DLQ generation for `reliability.retry_count == 0` and supported `dlq.mode` values.
- Verified source shapes: live Boomi exports for `catcherrors` / `doccacheload` and `processcall`.
- Still blocked/deferred: end-to-end runtime failure-row proof and R1b `retry_count > 0`.

## File-By-File Changes

### `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md`

Update the stale line around `320` that says `#51 (reliability): Try/Catch/DLQ proof remains out of scope — do not attempt`.

Replace it with an epic-level criterion 4 closeout note:

- State that #51 is closed and shipped R1a emitter support.
- Record that #51 verification was emitter-level live QA, not an end-to-end runtime failure-row proof.
- Mark the epic #9 criterion 4 path as `BLOCKED_WITH_EVIDENCE`.
- Cite the existing evidence from #51:
  - live-exported `catcherrors` / `doccacheload` shape from component `dff0bf83…`
  - live-exported `processcall` shape from component `7b19baeb…`
  - emitter live-QA results `62/0` and `10/0`
  - shipped commits `313ce04` and `b5c275b`
- Keep the #66 green orchestration run evidence intact:
  - package `3acd5ef7…`
  - deployment `577afecb…`
  - execution `execution-8e811200…`
  - terminal status `COMPLETE`
  - logs include `download_url` and excerpt
- Explicitly say no new live mutation is being attempted as part of closeout.

### `docs/MCP_TOOL_DESIGN.md`

Update the stale Try/Catch/DLQ/retry language around line `150`.

Replace the “fully gated” description with the current shipped split:

- `reliability.retry_count == 0` with `dlq.mode` in `{document_cache_ref, error_subprocess_ref}` is supported.
- The implementation emits verified Boomi Try/Catch/DLQ shapes using live-exported reference XML.
- DLQ `$ref` plan-time type checking exists and can return `PROCESS_REF_TYPE_MISMATCH`.
- `reliability.retry_count > 0` remains gated by `PROCESS_RETRY_UNVERIFIED`.
- End-to-end runtime failure-row proof remains blocked at epic closeout unless separately performed later.

Do not change tool API contracts or imply that retry `1..5` is supported.

### `docs/INTEGRATION_AUTHORING_ROADMAP.md`

Update the M3 roadmap status around line `154`.

Change M3 from `Next` to `Done` or equivalent completed status, with a short closeout annotation:

- `orchestrate_deploy` is built and public.
- Exit criteria 1, 2, and 3 are satisfied by #66 live QA.
- Exit criterion 4 is resolved by the permitted blocked-with-evidence path.
- R1a Try/Catch + DLQ emitter behavior is shipped through #51.
- R1b process retry remains blocked/deferred under `PROCESS_RETRY_UNVERIFIED`.

If the roadmap has an exit-criteria checklist, mark:
- `build_integration apply` to deployed/tested via one tool call: done.
- idempotent retry for existing package/deployment: done.
- structured deployment/test errors with diagnostics: done.
- retry/DLQ failure-row proof: blocked with recorded Boomi evidence, accepted for epic closeout.

### New or Existing Closeout Evidence Section

Add a concise epic #9 closeout record in the most appropriate existing docs location. Prefer appending to `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md` if it already serves as the M3 evidence ledger. If the repo already has an issue closeout/evidence convention, follow that instead without creating a new convention.

Record:

- Title: `Epic #9 Closeout Evidence`
- Status: `CLOSED_BY_DOCUMENTED_EVIDENCE`
- Date: use the implementation date when editing, not an invented historical date.
- Exit criterion 1 proof: #66 green run shows apply-to-deploy/test through `orchestrate_deploy`.
- Exit criterion 2 proof: #65/#66 hardened idempotent package/deployment handling and successful retry-safe behavior.
- Exit criterion 3 proof: #65 structured error codes and diagnostics; #66 successful terminal execution with logs.
- Exit criterion 4 resolution: `BLOCKED_WITH_EVIDENCE`.
- Criterion 4 evidence:
  - #51 closed with R1a emitter-level Try/Catch + DLQ.
  - Boomi shapes were transcribed from verified live exports: `dff0bf83…` for `catcherrors` / `doccacheload`, `7b19baeb…` for `processcall`.
  - #51 QA verified emitted XML, not runtime failure-row movement.
  - R1b retry `1..5` remains gated by `PROCESS_RETRY_UNVERIFIED`.
  - No live runtime failure-row proof was performed for epic closeout.
- Final statement: epic #9 can close without feature-code changes because criterion 4 explicitly allows a documented blocked path.

## Code And Tests

No code changes are planned.

Do not touch:
- `src/boomi_mcp/categories/deployment/orchestration.py`
- MCP tool registration
- `ProcessFlowBuilder`
- golden XML fixtures
- unit tests

No test changes are needed because this is a documentation/evidence reconciliation task. If implementation later changes code unexpectedly, that would require the repo’s full QA workflow, but this plan intentionally avoids that scope.

## Assumptions

- The closeout should not perform live MCP calls or mutate the Boomi account.
- The accepted criterion 4 resolution is the documented blocked-with-evidence path, not a new runtime proof attempt.
- R1a DLQ emitter behavior is shipped and should be documented as supported only for `retry_count == 0`.
- R1b process retry remains explicitly blocked/deferred and must not be represented as complete.
- The final implementation should be a minimal documentation diff only.
