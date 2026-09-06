I’ll compare the implementation with the plan, focusing on the continuation grammar, recovery contract, invariants, and declared departures. I’ll use targeted tests where needed and leave the full suite alone.
I’m using the receiving-code-review skill’s verification discipline to assess the departures against the code. The checkout is clean at `ae03d1d`; parallel reviews are covering the grammar, CFG invariants, and archived evidence.
I’ve reproduced two gaps so far: the continuation grammar rejects a legal root prefix before multiple handlers, and several canonical Notify/catch-call errors use different codes from the plan. I’m also checking the independent invariant controls, since correct emitted output alone doesn’t prove those guards work.
The recovery contract is faithful: Notify-only prefixes, flags checked by value, and an exemption confined to catches. The invariant review found gaps, though: both CFG and plan checks accept a consistently reordered catch block and false recovery flags. I also reproduced a JSON-array Notify template that passes full compilation but emits `{1}` inside quotes, leaving the caught error unbound.
The core recovery-call design is realized, but several explicit plan requirements remain unmet. I found these **Standard plan-fidelity issues**:

1. **The Continue grammar is too restrictive.** `[connector_call, handler(Continue), handler(Stop)]` is rejected as an orphan continuation, although both the existing prefix and the handler chain are individually legal. The planned `source → Map → handler → consumer` form also remains rejected. The chain recognizer excludes all root prefixes. Preserve those prefixes and validate the handler suffix separately. [process_ir.py:2763](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:2763)

2. **Continuation containment does not identify the authorized successor.** It accepts any root Map or handler. A three-handler CFG consistently changed to execute authored siblings `0 → 2 → 1` passes both invariant checkers. Requiring a declaring handler to escape is sound, but the escape must reach the exact next sibling, through its permitted separator. [invariants.py:840](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/invariants.py:840)

3. **The planned independent emission safeguards are missing.** Two concrete mutations pass both checkers:
   - Interleave catch1 before handler2, consistently updating CFG and plan.
   - Change either lowered recovery flag to false and generate a matching plan.

   Comparing plan order and inputs with the same CFG cannot detect these lowering defects. Independently derive main-first/catch-block order and require recovery flags to remain true. [Ordering check](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/invariants.py:1139), [input correspondence check](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/invariants.py:1213)

4. **Notify validation still admits a template that defeats binding.** `["meta.base.catcherrorsmessage"]` passes full compilation and emits `'["{1}"]'`: the placeholder remains inside MessageFormat quotes. The brace ban misses JSON arrays. Separately, a constructed Notify input with `message_template=None` raises raw `AttributeError` during emission preflight instead of the required `PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID`. Complete template validation at both boundaries. [Model guard](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:1711), [emitter preflight](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/emitter_registry.py:637)

5. **Canonical diagnostic mappings are incomplete.** Reproduced:
   - Blank/missing-token Notify → `PROCESS_IR_SCHEMA_INVALID_CARDINALITY`, rather than `PROCESS_IR_SCHEMA_INVALID`.
   - Root Notify → `PROCESS_IR_CAPABILITY_UNSUPPORTED`, rather than `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY`.
   - Catch-step ProcessCall → body-placement refusal, rather than the required connected-call identity.

   The last discrepancy comes from the translator still recognizing only Branch legs and Decision true arms. Retaining the legacy Notify identity does not justify these canonical differences. [Notify validation](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:1687), [catch-call translation](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:3795)

6. **Required golden migration and independent comparisons are unfinished.** Both existing Notify corpus entries still use the legacy builder. The single-region canonical test compares shapes; the new sequential fixture omits the Map and changes the graph, so it does not reproduce `golden-000005`. Notify’s XML/doctrine witnesses also remain legacy-owned. Additionally, the claimed canonical comparison against the independent B2 capture is absent. Complete the planned canonical envelope renderers and full-byte comparisons while preserving the existing fixtures and IDs. [Legacy corpus entries](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_wave_gate_golden_corpus.py:744), [unsupported comparison claim](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_wave_gate_golden_corpus.py:1479)

7. **Served ordering guidance contradicts Map admission.** The projection says nothing may follow a handler except another handler, omitting the permitted intervening Map. [process_ir_projection.py:637](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_projection.py:637)

My judgments on the declared departures:

| Departure | Judgment |
|---|---|
| **1. Retain legacy Notify identity** | **Sound compatibility choice.** Canonical mappings still need finding 5 fixed. |
| **2. Bound regions by success/catch edges** | **Sound as conservative replay analysis.** Excluding downstream protected writes was unsafe. The archived second-region isolation control supports that control’s claim; it does not establish every stronger claim about retries originating in an earlier handler. |
| **3. Leave validation policy unchanged** | **Sound.** It is an exemption registry; absence keeps the new code blocking. |
| **4. Leave emission ordering functions unchanged** | **Sound for generation.** Correct CFG order already yields the intended allocation. Independent validation remains missing. |
| **5. Leave `_walk_controls` unchanged** | **Sound under the admitted grammar.** Catch-local validation and ancestry handling cover the newly admitted calls. |
| **6. No Notify effect-dispatch arms** | **Sound.** Existing neutral fallthrough preserves the modeled state/effect behavior. |
| **7. Added guards and translation** | Discriminator entries and closed-set `_as_compile_error` are **sound**. Rejecting binding-breaking templates is justified, but the brace guard is incomplete as reproduced above. |

Directly answering the three main questions: **Continue is the right structural terminal, but its surrounding grammar is incomplete. Active-depth handling preserves the tested nesting limits; containment is insufficiently precise. The Notify-only, terminal recovery contract with both flags checked by value is faithful to the plan.**

Other explicit planned work remains: derive the catch union from `ControlBodyStepV1`’s actual members and independently test that relationship; update the architecture narrative; add the specified recovery/chain state, grant, revocation, and relocation regressions. The current catch membership itself is correct.

**383 focused tests passed**, plus targeted reproductions of the findings. The full suite was not rerun; the checkout remains unchanged. Archived oracle and runtime controls are substantive. Python 3.11 CI and final-wave validation remain subsequent completion gates.

VERDICT: ISSUES FOUND
