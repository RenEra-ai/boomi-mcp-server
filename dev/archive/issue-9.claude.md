# Epic #9 Closeout Implementation Plan

## Summary
Docs-only closeout for epic #9. `orchestrate_deploy` is already built, registered, hardened, and live-QA'd (#60–#66 + #51 all closed/merged); no feature code changes. The work edits three docs to reconcile them with the shipped state — resolving exit criterion 4 via the allowed "blocked with recorded Boomi evidence" path, splitting the stale "fully gated" Try/Catch/DLQ language into the actual R1a-shipped / R1b-still-gated reality, flipping M3 from "Next" to "Done", and appending an "Epic #9 Closeout Evidence" ledger entry to the M3 runbook. No source, no live mutation, no new evidence runs — only reconciliation of existing recorded evidence.

## File-by-file

### 1. `docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md`

**Edit A — the stale #51 follow-up line (line 320, "Follow-up issues" section).**
Current text:
```
- **#51 (reliability):** Try/Catch/DLQ proof remains out of scope — do not attempt
  it from this runbook.
```
Replace with text that records the shipped R1a state and points to the closeout ledger: #51 is CLOSED, shipping R1a Try/Catch + DLQ emitter support for `retry_count == 0` with `dlq.mode` in `{document_cache_ref, error_subprocess_ref}` (commits 313ce04 + b5c275b); #51's verification was emitter-level live QA (catcherrors/DLQ shapes from live-exported reference XML), not an end-to-end runtime failure-row execution; end-to-end runtime failure-row proof and `retry_count > 0` (R1b) remain blocked/gated under `PROCESS_RETRY_UNVERIFIED`; epic #9 criterion 4 is therefore recorded `BLOCKED_WITH_EVIDENCE` (see the closeout ledger below). Keep the "do not attempt new live mutation from this runbook" intent.

**Edit B — append the "Epic #9 Closeout Evidence" ledger.**
Placement: append a new top-level section `## Epic #9 Closeout Evidence` at the very end of the file, after the existing `## Safety reminders` section. Keeps the existing green-run evidence block intact. Contents:
- **Status:** `CLOSED_BY_DOCUMENTED_EVIDENCE`, dated **2026-06-08**.
- **Criterion 1** (one-call build→deploy→test): #66 green run — package `3acd5ef7…`, deployment `577afecb…`, execution `execution-8e811200…`, terminal `COMPLETE`, logs `retrieved` with `download_url` (cite the existing in-file evidence block, do not duplicate raw log).
- **Criterion 2** (idempotent retry-safe): #65 hardening + #66 reuse behavior.
- **Criterion 3** (structured error codes + diagnostic context): #65 structured-error envelope (`error_code` / `failed_stage` / `prior_stage_summary` / `next_step`) + #66 Step 5 `BUILD_ID_UNKNOWN` + Step 4 logs.
- **Criterion 4** (retry/DLQ failure-row proof): `BLOCKED_WITH_EVIDENCE`. R1a verified at emitter level for `retry_count == 0` and supported `dlq.mode`; live-exported reference shapes verified — catcherrors/doccacheload `dff0bf83…`, processcall `7b19baeb…`; emitter live-QA 62/0 and 10/0; DLQ `$ref` plan-time type check returns `PROCESS_REF_TYPE_MISMATCH`; commits 313ce04 + b5c275b; **still blocked/deferred:** end-to-end runtime failure-row execution and R1b (`retry_count > 0`, gated by `PROCESS_RETRY_UNVERIFIED`).
- **Closing statement:** epic #9 can close without new feature code because exit criterion 4 explicitly permits a "documented blocked path with recorded Boomi evidence", and no new live mutation is attempted for this closeout.

(Confirm `dff0bf83…`, `7b19baeb…`, and the 62/0 & 10/0 counts against the #51 artifacts/commits before writing them as final; cite only IDs actually recorded. If any cannot be confirmed, cite the commit SHAs and omit the unverified hash.)

### 2. `docs/MCP_TOOL_DESIGN.md`

**Edit — the "fully gated" reliability bullet (line 150).**
Current text:
```
- **Audit/provenance is opt-in metadata in v1**, not a mandatory always-on shell. Try/Catch, DLQ, Branch, Process Call, and retry behavior stay gated until their Boomi XML and live behavior are verified (see `docs/INTEGRATION_AUTHORING_ROADMAP.md` M5 and the reliability follow-up #51).
```
Replace the gating clause with the shipped split — keep Branch/Process-Call-as-shape and the M5 audit/provenance framing where still accurate, but correct Try/Catch/DLQ/retry to: Try/Catch + DLQ emission is shipped (R1a, #51) for `retry_count == 0` with `dlq.mode` in `{document_cache_ref, error_subprocess_ref}`, emitting verified Boomi Try/Catch/DLQ shapes compiled from live-exported reference XML; the DLQ `$ref` binding gets a plan-time type check returning `PROCESS_REF_TYPE_MISMATCH`; `retry_count > 0` remains gated by `PROCESS_RETRY_UNVERIFIED`; end-to-end runtime failure-row proof remains blocked at M3/#9 closeout. Do **not** imply retry 1..5 is supported. Update the `#51` reference from "follow-up" to "shipped R1a (closed)".

### 3. `docs/INTEGRATION_AUTHORING_ROADMAP.md`

Two stale spots (architect cited only line 154; line 22 is the same status in the table and must match):

**Edit A — roadmap status table (line 22).**
Current text:
```
| M3 Deploy and Test Orchestration | 2026-06-08 | 2026-06-12 | Next; split into #60-#66 plus reliability follow-up #51 |
```
Change the Status cell to `Done 2026-06-08` with the same closeout-annotation style as the M1/M2 rows (e.g. `Done 2026-06-08 (#60-#66 closed; #51 R1a shipped; parent #9 closed by documented evidence)`). Keep date columns unchanged.

**Edit B — M3 section status + exit criteria (lines 152–180).**
- Line 154 current text:
  ```
  Status: Next — due 2026-06-12. Parent #9 is split into #60-#66. Reliability follow-up #51 is also assigned to M3 because runtime failure proof requires verified Try/Catch/DLQ behavior, but #51 is not a child of #9.
  ```
  Replace `Status: Next — due 2026-06-12.` with `Status: Done 2026-06-08.` and add a one-line closeout annotation: `orchestrate_deploy` built + public (#60–#66 closed); exit criteria 1/2/3 satisfied by #66; criterion 4 resolved via blocked-with-recorded-evidence; #51 shipped R1a (`retry_count == 0`); R1b (`retry_count > 0`) remains gated under `PROCESS_RETRY_UNVERIFIED`. Keep the existing factual sentence that #51 is not a child of #9.
- Exit-criteria list: the 4th bullet already reads "either verified live through #51 or explicitly blocked with recorded Boomi evidence" — no wording change. Add one line under the list: `Closeout (2026-06-08): criteria 1–3 satisfied (#66); criterion 4 = blocked-with-recorded-evidence accepted — see docs/M3_ORCHESTRATE_DEPLOY_LIVE_QA.md "Epic #9 Closeout Evidence".` Do not rewrite the criteria bullets.

## Test plan
Docs-only — no unit tests change; `orchestration.py`, MCP registration, `ProcessFlowBuilder`, golden fixtures, and unit tests are explicitly untouched.

Per CLAUDE.md the completion workflow (boomi-qa-tester QA + Codex review) applies to every completion point including docs. Adapted to docs-only:
- **QA-equivalent (no tool behavior regressed):** the `boomi-qa-tester` agent tests TOOL behavior via live calls — N/A for a docs edit. Relevant assertion: "no tool behavior regressed". Satisfy with: confirm `git status` shows only the three `docs/*.md` files changed (zero `src/` diff), run `PYTHONPATH=src python -c "import server"` (editable `.pth` is stale — use `PYTHONPATH=src`) + confirm `orchestrate_deploy` is registered, and optionally run the orchestrate_deploy contract/wrapper tests with `PYTHONPATH=src pytest`. No live Boomi mutation.
- **Codex review** covers substance: doc accuracy — R1a-shipped / R1b-gated faithful, no overstatement of retry support, every cited ID/SHA real, closeout date 2026-06-08. Follow Stage-2: commit docs-clean baseline first, then Codex `review --wait`, apply `Skill: receiving-code-review`, re-scope any fix re-review to the delta only.

## Deviations from architect plan
- **Addition:** roadmap table line 22 also carries M3 status; both line 22 and line 154 must flip to "Done" for internal consistency.
- **Scoping clarification:** the M3 exit-criteria 4th bullet already permits the blocked-with-evidence path verbatim, so annotate rather than rewrite (minimal diff).
- Otherwise follows the architect design (file set, no-feature-code, closeout ledger in M3 runbook, 2026-06-08 date).
