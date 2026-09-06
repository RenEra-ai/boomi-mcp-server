I’ll review the evaluation-2 fix delta, check the golden-corpus deferral, and classify the remaining items against the final-round policy. I won’t rerun the full suite.HEAD matches `f7b3687`, and the working tree is clean. I’m using the receiving-code-review skill to assess the fixes and refutations, with parallel checks of containment, ordering, and deferred coverage.The focused Notify/recovery tests pass: 73 tests. I’m checking the diagnostic fix across the other root grammar branches, since the new Notify check runs after some branches can return.The containment and ordering fixes reject mutations that the previous HEAD accepted. Two gaps remain: Notify before a handler chain still gets the generic root diagnostic, and the new golden-deferral test hits an action mismatch rather than the missing POST capability. The POST capability gap itself is real; #156 does not need to add that row.**Findings 1–4 and 7 are closed. Finding 5 remains partially open. Finding 6 has a sound deferral basis, but its evidence and recorded disposition need correction.** I found no unresolved Critical defect in the reviewed delta.

Reviewed `48d7ea0..f7b3687`. **73 focused tests passed**, plus targeted mutation probes. I did not rerun the full suite or modify the repository.

| Finding | Evaluation-3 judgment |
|---|---|
| **1. Continue/cache guard** | **Closed.** Both the single-handler and chain forms now reject `call → cache_put → call` with the cache-consumption refusal. |
| **2. Map handoff containment** | **Closed.** A consistently rewired and renumbered four-handler chain passes the previous checker but fails HEAD with `PROCESS_IR_COMPILE_ERROR_REGION_INVALID`; the valid control passes. |
| **3. Independent ordering** | **Closed.** Consistently mutated CFG/plan pairs with swapped catch blocks or catches interleaved into the main allocation now fail. Valid controls pass. |
| **4. Notify templates** | **Closed.** Ordinary `[ERROR]` text remains legal; JSON objects and arrays fail emitter preflight as well as authoring validation. |
| **5. Root Notify identity** | **Partially closed; Standard blocking contract defect remains.** The chain branch bypasses the new check. |
| **6. Golden migration** | **Deferral justified; not a refutation of the missing coverage.** Correct the witness, fixture assertions, and deferral record below. |
| **7. Served Map ordering** | **Closed.** No reason to reopen it. |

The remaining correction to finding 5 is concrete: `[call, notify, handler(continue), handler(stop)]` returns `PROCESS_IR_CAPABILITY_UNSUPPORTED` at `/body`. The equivalent ordinary root correctly returns `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` at `/body/steps/1`. The [chain early return](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:2963) precedes the new Notify check. Apply the placement check before that branch and pin both cases. This is incomplete closure of the existing finding.

**#156 should not add REST POST support merely to unblock these goldens.** The missing capability is real. However, two evidence corrections are required:

- The [new negative witness](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_notify_recovery.py:1309) asserts `post` against an authoritative **PATCH** operation. It exercises the action-mismatch refusal, which happens to use the same code. Use an actual POST operation symbol, such as the one in `_dlq_symbols()`.
- Changing the parity documents to `action: "send"` did **not** make them correct. `action` is an optional assertion against the operation’s authoritative action, not a generic get/send vocabulary. Both REST operations are POST: omit the assertion or use `post`. A simulated POST capability exposes the remaining `send` mismatch.

Record migration as **`blocked-by-mechanism`**, replacing the [ledger’s `finding-refuted` disposition](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_156_AUDIT_LEDGER.md:131). Enumerate the two corpus migrations and their full-envelope acceptance criteria in a filed, sequenced follow-up, with the checkpoint recorded in-tree. Generic ownership by the already-closed #155 does not supply that disposition.

For the three explicitly open items:

- **Architecture narrative: advisory; follow-up acceptable.** The [old Try/Catch narrative](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/PROCESS_IR_V1.md:213) remains inaccurate, but it is static documentation. Apply the permitted prose batch or record the remaining work; it does not independently block this slice.
- **Recovery/chain regressions: blocking validation work for this slice.** These are agreed checks of newly admitted graph forms, not evidence of an existing Critical runtime defect. The finite remaining cases are:
  1. A recovery child’s state read cannot use its own failed try’s writes; scope-entry and successful earlier-region writes can satisfy it, while earlier recovery-only writes cannot.
  2. A valid continuing-chain write succeeds with its grant; transplanting that grant across call path/sibling, operation, or root fails.
  3. A legal recovery/chain request reaches public planning and materialization with child references/dependency order intact; revoking required evidence after planning prevents mutation.
  4. Reuse that request across two account bindings to verify relocatable identity and correctly rebound child references.

  Existing tests already cover retry membership, later protected-write refusal, and exclusion of recovery-only writes. Those do not need another coverage round. The existing [relocation test](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_materialization_plan.py:107), however, uses the older document form.
- **Notify witness re-homing: nonblocking ownership debt; follow-up acceptable.** Canonical registration/emission and frozen Notify bytes already have independent coverage. Moving the remaining XML/doctrine witnesses matters for legacy removal, but their present ownership alone is not a behavior defect.

The catch union derivation and its bidirectional pins are accepted as closed.

This ends the architect loop. Apply the finite final correction batch, run its affected validation and Stage-2 correction review to clean, and keep the final wave gate current. **No fourth architect evaluation is required.**

VERDICT: ISSUES FOUND
