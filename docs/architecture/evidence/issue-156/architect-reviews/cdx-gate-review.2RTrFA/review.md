I’ll review the fix delta against the seven findings and the plan, without rerunning the full suite. I’ll also check whether the new guards reject consistently mutated CFG and emission-plan inputs.
The supplied full baseline SHA does not resolve; `ae03d1d` resolves to `ae03d1de1c41adc5376151d9f741aba3b714b952`, immediately before HEAD. I’m reviewing that delta and applying the receiving-code-review skill to verify the closure claims.
The recovery flags now have a direct contract check. The ordering check still compares the plan’s order with the CFG’s order; I’m testing whether both can agree on an allocation that violates the required main-first, ordered-catch layout.
Two gaps reproduce with valid controls: mapped continuation can still reach the wrong handler, and consistently reordered catches still pass both invariant checkers. The 248 focused Notify/recovery and error-handling tests pass. I’m finishing the diagnostic and golden-migration judgments.
The delta still has issues. Finding 1 fixes the original refusal but weakens another guard; findings 2–6 remain partially open; finding 7 is closed.

Reviewed `ae03d1de1c41adc5376151d9f741aba3b714b952..48d7ea09654ae8e1faaafdb9ee4bc88243cc67c8`. The supplied full baseline SHA did not resolve.

1. **Continue grammar — original defect fixed; new guard bypass.** The prefix check validates vocabulary but skips the existing cache-consumption rule. Reproduced:

   - `[call, cache_put, call, handler(stop)]` → cardinality refusal.
   - Identical prefix followed by `[handler(continue), handler(stop)]` → parsing **and public compilation pass**.

   Add to Cache consumes the document stream. Preserve the existing prefix constraints, including the required following cache read, before taking the chain’s early return. [Prefix validation](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:2841)

2. **Containment — partially fixed.** Direct handler handoffs now identify the successor. A Map handoff still stops traversal immediately, leaving the Map’s successor unchecked—contrary to the new comment.

   A four-handler mapped chain consistently rewired to execute authored handlers **0 → 4 → 2 → 6** passes **both CFG and plan invariants**, with every node reachable. Require the separator’s successor to be the next authored handler too. [Containment escape](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/invariants.py:862)

3. **Independent emission safeguards — flags fixed; ordering still required.** I verified that independently changing either recovery flag now produces `PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID`.

   However, both checkers still accept—and emission produces XML for—consistently mutated CFG/plan pairs that:
   
   - allocate catch1 before the second handler;
   - swap the complete catch blocks.

   Flag validation cannot establish allocation order. Independently deriving main-first, contiguous, handler-ordered catch blocks remains necessary. [CFG-order comparison](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/invariants.py:1197)

4. **Notify templates — partially fixed, with a new rejection regression.** The parser rejects JSON arrays, and the `None` preflight fix works. Two problems remain:

   - A mutated Notify emission input containing `["meta.base.catcherrorsmessage"]` still passes preflight and emits the quoted, nonbinding `{1}`. Apply the binding rule at this boundary too. [Preflight](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/emitter_registry.py:646)
   - Rejecting **every** leading `[` also rejects safe, previously accepted text such as `[ERROR] caught meta.base.catcherrorsmessage`. The renderer does not classify that as JSON and correctly emits `[ERROR] caught {1}`. Match the actual quote-wrapping condition while preserving ordinary bracket-prefixed text. [Template predicate](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:1649)

5. **Diagnostics — two fixes closed; root refutation rejected.** Schema-invalid template classification and catch-step connected-call classification are correct.

   S22 explicitly assigns `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` to Notify outside admitted catches. The generic root implementation does not refute that contract, and this newly introduced canonical node has no pre-slice root diagnostic to preserve. Handle Notify explicitly without changing unrelated kinds. [Root refusal](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:3084)

6. **Goldens — useful comparisons added; deviation insufficient.** The permanent double-guard shape pin and independent B2 comparison close those subfindings.

   The new comparison still bypasses compilation/materialization and checks only `<shapes>`. The planned canonical corpus route exercises normalization, compilation, late binding, materialization, and complete envelope bytes. Changing the renderer leaves the frozen expected bytes independent; keeping a separate legacy differential preserves legacy coverage. These are compatible requirements. [New comparison](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_notify_recovery.py:1208), [canonical envelope pattern](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_wave_gate_golden_corpus.py:990)

   Notify’s XML and doctrine witnesses also remain legacy-owned.

7. **Served Map ordering — closed.** The optional single `map_ref` is now stated. The replacement Branch witness is also sound.

Previously identified plan work remains unchanged: deriving the catch union from the shared union and independently testing that relationship; correcting the architecture narrative; and adding the specified recovery/chain state, grant-isolation, revoked-evidence, and relocation regressions.

**248 focused tests passed**, alongside targeted mutation probes. The full suite was not rerun; the repository remains unchanged.

VERDICT: ISSUES FOUND
