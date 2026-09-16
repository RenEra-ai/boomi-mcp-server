I’ll compare the final tree with amendment 3 and the surviving earlier requirements, then judge C1–C25 and the recorded evidence dispositions. I’ll keep this read-only and use targeted tests only where needed to verify a concrete concern.
The ledger records substantial changes to the original design. I’m checking the current behavior behind them, especially cache-origin profile checks, child state transfer, and calls reached through an interposed Decision, where a correction could leave a required case unimplemented.
The focused probes confirm several plan gaps: an interposed Decision bypasses prefix refusal for No Data and unknown children; UNKNOWN profiles pass through some required checks; and a child retrieving a parent-populated cache receives its profile facts but loses cached DDP writer proof. The repeated-invocation check also rejects a stable DPP overwrite instead of checking whether later invocations remain valid.
One probe hit a looping mocked metadata pager. The sandbox blocked interrupting that process, and the permission request was rejected. I’m continuing with bounded tests that stub the pager explicitly.
The typed route derives child contracts, but the raw canonical preflight omits them: the same admitted passthrough-prefix fixture passes typed planning and fails raw preflight. I’m finishing the emission and evidence checks. The archived A8 run includes the required GET/PATCH paths and mapped payload, so the expressibility proof itself is present.
The account-binding refusal and unreadable-account behavior match the recorded owner decision. The remaining concerns are plan-specific: UNKNOWN proof bypasses, child cache-property transfer, repeated No Data invocation analysis, and the required shared-renderer guard. I’m checking revision coverage before the final verdict.
The targeted checks passed: 20 tests covering C25, unreadable-account handling, A8, emission aliases, capture hashes, and active cache goldens. An in-memory mutation also changed cached-writer validation without changing `compiler_revision`, confirming a separate revision-coverage gap.
Reviewed files: `docs/architecture/{ISSUE_184_AUDIT_LEDGER.md,M12_COMPATIBILITY_INVENTORY.md,PROCESS_IR_V1.md,PROCESS_IR_COMPILER_V1.md}`; `src/boomi_mcp/authoring/{contract.py,process_entry.py,process_ir_effects.py,process_ir_projection.py,process_materialization.py,workflow.py}`; `src/boomi_mcp/models/{process_ir.py,process_ir_document_semantics.py}`; `src/boomi_mcp/compiler/process_ir/{body_capabilities.py,connector_resolution.py,contracts.py,emitter_registry.py,entry_policy.py,execution_profile.py,invariants.py,lowering.py,pipeline.py}`; `src/boomi_mcp/compiler/process_ir/semantic_validation/{contracts.py,findings.py,lineage.py,pipeline.py}`; `src/boomi_mcp/categories/integration_builder.py`; `src/boomi_mcp/categories/deployment/orchestration.py`; `src/boomi_mcp/categories/components/{canonical_process_apply.py,process_component_materializer.py,process_graph_verifier.py}`; `src/boomi_mcp/categories/components/builders/process_flow_builder.py`; `src/boomi_mcp/categories/components/builders/process_emitters/{legacy.py,rendering.py}`; `src/boomi_mcp/recipes/materialization.py`; `src/boomi_mcp/patterns/primitives/throw_exception.py`; `tests/test_issue_184_{native_sequences,child_entries,stream_profiles,document_emission,component_identity,cutover,ddp_invalidation,passthrough_entry}.py`; `tests/fixtures/process_ir/issue184/PROVENANCE.md`; `agents/reports/{2026-09-13-issue-184-stage1-r1.md,2026-09-13-issue-184-stage1-r2.md,2026-09-15-issue-184-stage2-r18.md}`.

## Findings

1. **[P1] UNKNOWN profiles bypass property-source and cache-write proof.**  
   [lineage.py:1417](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:1417), [lineage.py:1551](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:1551).  
   Base §4 requires checking every “profile-valued `set_ddp`/`set_dpp` source”; amendment 3 A3 preserves “per-path map, cache-write and profile-source checks.” Both compiler routes accept a scheduled Branch leg containing `message("not-json") → set_ddp X(profile=P2, element_id=3) → stop`. They also accept `GET_UNDECLARED → terminal cache_put CACHE_P1`, where GET declares no output profile and the cache declares P1. The checks reject conflicting **known** identities but accept UNKNOWN. Required profile diagnostics are missing at `/source_values/0/profile_ref` and `/terminal/cache_ref`.

2. **[P1] Cache retrieval erases the obligation to check a typed connector input.**  
   [lineage.py:1467](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:1467).  
   Base §4 requires checking every “declared-input `documents_required` call”; amendment 1 preserves D4 only for “ordinary connector-to-connector non-validating profile metadata.” Both routes accept:
   ```
   Leg 1: GET(P1) → map(P1→P1) → terminal cache_put CACHE_P1
   Leg 2: cache_get CACHE_P1 → PATCH(input=P2) → stop
   ```
   P1 and P2 resolve to different components. Retrieval changes the stream origin from `map` to `cache`, bypassing the comparison. This mapped payload requires `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` at the PATCH’s `/operation_ref`; C5’s blanket cache exemption exceeds D4.

3. **[P1] An interposed Decision bypasses UNKNOWN-child and No Data prefix refusals.**  
   [lineage.py:165](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:165).  
   Base §2 explicitly requires the same proofs for a prefix “routed through an interposed Decision into an empty-prefix call”; amendment 1 rule 3 says to preserve the “root-to-leaf native-work marker.” A passthrough parent with `map(P1→P2) → Decision → TRUE(empty steps) → process_call` compiles both when the child is an opaque external reference and when it is an inspected No Data child. `_call_prefix` examines only the immediate body and returns no prefix. The required placement refusal at the nested `/true_arm/terminal` disappears. [The existing test explicitly expects this bypass.](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_184_child_entries.py:294)

4. **[P1] An opaque grandchild becomes a document-independent forwarding contract.**  
   [lineage.py:1428](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:1428), [process_ir_effects.py:1023](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:1023).  
   Amendment 1 §2 requires: “Opaque or undeclared consumption cannot become `NONE`.” For:
   ```
   Parent: passthrough → Branch leg [map(P1→P2)] → call MID
   MID:    passthrough → call EXTERNAL
   ```
   EXTERNAL has a process symbol but no inspectable root. MID receives `document_requirements=()` despite `state_known=False`, and both parent and MID compile. The unknown call contributes no document requirement, so the new prefix incorrectly treats MID as requiring no payload proof.

5. **[P2] Child cache contracts discard cached document properties and writer provenance.**  
   [lineage.py:1960](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:1960).  
   Amendment 3 §7 requires freezing cached “guaranteed DDP keys” and “server-derived writer alternatives”; §8 requires both child forms to receive the execution’s cache facts. A parent can stage `GET → set_ddp X("/clients/" + DPP key) → terminal cache_put`, then invoke an empty-prefix No Data child containing `cache_get → GET(path_binding=X) → stop`. The parent passes, but the child receives `PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED` at `/body/steps/1/path_binding`. Its seeded cache cohort is forcibly UNKNOWN. The equivalent in-process staged graph passes. This is cached-only provenance, so the OPEN N×M current-overlay boundary does not justify refusal.

6. **[P2] Child state transfer omits guarantees and replaces repeated-invocation analysis with a blanket refusal.**  
   [process_ir_effects.py:1005](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:1005), [lineage.py:1683](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:1683).  
   Amendment 3 §8 requires “possible versus guaranteed execution-state effects”; amendment 1 rule 8 requires analysis “using the same finite state/profile lattice until stable.” Neither is realized by C20:
   - A waited, abort-on-error No Data child that sets DPP K on every path validates, but a later parent Branch leg reading K gets `PROPERTY_READ_BEFORE_WRITE` unless an effect declaration is added.
   - A No Data child that reads established K and writes K again is refused for multiple invocations, although K remains established and the lattice immediately stabilizes.

   Recording only possible mutations loses provable guarantees and rejects stable compositions.

7. **[P2] Raw canonical preflight omits the derived child context.**  
   [integration_builder.py:8722](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:8722).  
   Base §4 requires “the same derived profile facts and child capabilities through raw canonical preflight and dry emission”; amendment 3 §8 retains that transport obligation. A two-root request with a scheduled parent Branch leg `message → call child` and child `passthrough → message → stop` passes typed planning. Passing its normalized spec through raw `_build_canonical_plan` instead raises placement unsupported at `/body/steps/0/legs/0/terminal`. This route neither derives nor supplies child capabilities to `build_materialization_plan`.

8. **[P2] Shared cache renderers still emit forbidden outgoing wires.**  
   [rendering.py:614](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/builders/process_emitters/rendering.py:614), [rendering.py:911](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/builders/process_emitters/rendering.py:911).  
   Amendment 3 §4 explicitly requires these renderers to “reject outgoing transitions for load/remove even when authoring validation is bypassed.” Calling either renderer with a `ShapeRenderContext` containing one transition to `shape2` returns XML containing `toShape="shape2"`. Registry and legacy-adapter guards work, but the required shared-renderer guard is absent. Consequently, C13’s renderer claim and the §12 sibling-coverage claim are incomplete.

9. **[P2] Retrieve-overlay behavior is not covered by `compiler_revision`.**  
   [contract.py:1260](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/contract.py:1260).  
   Amendment 3 §10 requires revision material to include “overlay policy” and behavioral perturbations demonstrating that behavior changes move revisions. An in-memory mutation making `_overlay_cache_read` discard cached writer alternatives changes a legal staged bound-path graph from successful compilation to `DYNAMIC_PATH_DDP_NOT_ESTABLISHED`. Nevertheless, `_compiler_revision()` remains exactly `sha256:e77d78674859a27566bc3943753ba5aca6f472d32097aa0db8f8621875d40cdd`. The revision records table labels and the singleton token, but its behavior cases do not exercise cached-writer restoration.

## Required disposition checks

- **Both child forms:** entry emission and form-specific invocation handling exist; findings 3–7 prevent full child-contract acceptance.
- **Unreadable-account owner decision:** accepted. Plan/compile degrade with the advisory covering account bindings, write conflicts, name ambiguity/collisions, warnings and reused-create profile facts. Apply refuses an unjudged recompile before binding comparison or writes.
- **E0/E1:** the original public cache-successor and Exception-none defects have witnesses supporting their corrections. Complete structural closure is overstated by finding 8; cached-property transfer remains incomplete across child boundaries under finding 5.
- **A8 / inherited findings:** raw-shapes tests and the archived final-source public/live evidence establish the expressibility portion, including GET/PATCH paths and mapped payload. INH-EVAL-155-02/-02a should remain open until the remaining recorded gate and handoff obligations are discharged.

Twenty targeted tests passed. No files were changed. One mocked-pager probe, PID `29826`, remains running after its interrupt permission request was rejected.

## Plan corrections

- C1: accepted.
- C2: accepted.
- C3: accepted.
- C4: accepted.
- C5: rejected — finding 2.
- C6: accepted.
- C7: rejected — finding 1.
- C8: accepted.
- C9: accepted.
- C10: accepted.
- C11: accepted.
- C12: accepted for the bounded in-process overlay.
- C13: rejected — finding 8.
- C14: rejected as fully realized — finding 4.
- C15: accepted.
- C16: accepted; the independent transform preserves the required parity.
- C17: accepted.
- C18: accepted; no required legal removal composition is lost.
- C19: accepted.
- C20: rejected — finding 6.
- C21: rejected — findings 3 and 5.
- C22: accepted.
- C23: accepted.
- C24: accepted as revised by C25 batch 10 and the owner’s degradation decision.
- C25: accepted. The refusal resolves ambiguous written-component configurations; none of the required sequence admissions needs duplicate specs naming that component.

VERDICT: ISSUES FOUND
