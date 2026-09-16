I’ll review the correction delta against amendment 3, trace all nine prior findings and six revised correction rows, and run targeted local probes where needed. The checkout will remain unchanged; I won’t use network or Boomi tools.HEAD is `bb739a8` on a clean `codex/issue-184` checkout. I’m using the local architect-conformance-review skill to check each correction and its required sibling cases, starting with cache provenance and child contracts.The cache checks now share a rule for typed consumers, and the shared renderers have their own terminal guards. The child corrections also add cached-property requirements and execution-state guarantees. I’m checking those through forwarding calls and Branch/Decision paths, where an immediate-child fix could still miss a required case.The literal corrections are holding up so far, including the two scoped exceptions in finding 1. I’ve found two cases to probe around the new child guarantees: a nested call whose input is provably nonempty but not singleton, and a declared subprocess effect on a call that tolerates child failure.The 429 targeted correction tests passed. Two additional probes expose gaps: inserting a Message after a child’s second cache read drops a required cached-writer check, and a passthrough forwarder loses its child’s DPP guarantee despite a proven nonempty input. Both reproduce through both compiler entry points; the Message case also disagrees with the equivalent graph kept within one process.The baseline comparison confirms the cached-writer bypass is introduced by this delta: `80bdd30` refuses the child, while HEAD accepts both roots. The guarantee gap also reproduces through public plan and compile. Another 45 targeted checks passed, including the owner decisions, A8 parity, prefix cases, and review attestations. The checkout is still unchanged.Reviewed files: `docs/architecture/ISSUE_184_AUDIT_LEDGER.md`; `docs/architecture/evidence/issue-184/architect-reviews/cdx-gate-review.j1rWJg/review.md`; `src/boomi_mcp/authoring/{contract,process_ir_effects,process_ir_projection,process_materialization,workflow}.py`; `src/boomi_mcp/categories/integration_builder.py`; `src/boomi_mcp/categories/deployment/orchestration.py`; `src/boomi_mcp/categories/components/builders/process_emitters/rendering.py`; `src/boomi_mcp/compiler/process_ir/diagnostics.py`; `src/boomi_mcp/compiler/process_ir/semantic_validation/{context,contracts,findings,lineage}.py`; `src/boomi_mcp/errors.py`; `tests/test_issue_184_{child_entries,child_state_transfer,component_identity,cutover,document_emission,native_sequences,raw_route_context,revision_coverage,stream_profiles}.py`; `tests/test_issue_180_compile_entry_context.py`; `tests/test_process_ir_effect_declarations.py`; `tests/test_process_ir_semantic_lineage.py`; `tests/test_wave_gate.py`.

1. **[P1] A Message after re-caching drops a required cached-writer check.**  
   [process_ir_effects.py:1174](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:1174).

   Amendment 3 §7 requires: **“A bound path must pass for every possible selected writer. Never discard an inconvenient writer alternative.”**

   Concrete input:

   ```text
   Parent, ordered Branch legs:
     1. GET → set_ddp X(dynamic composition) → terminal cache_put C1
     2. GET → set_ddp X(static "/c/1") → terminal cache_put C2
     3. empty-prefix call to No Data child

   Child, ordered Branch legs:
     1. cache_get C1 → set_dpp Y(ddp X) → terminal cache_put C2
     2. cache_get C2 → message("m") → GET(path_binding=X) → stop
   ```

   Both compiler entry points accept both roots. The Message clears the retrieved-cache marker, so the derived contract omits C2’s bound-writer requirement. The caller’s static X therefore escapes validation despite remaining among the possible cached writers.

   Removing the Message correctly produces `PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT` at `/body/steps/0/legs/2/terminal/process_ref`. Flattening the same legs into one process also refuses the binding. Importing the baseline sources confirms `80bdd30` refuses the child; this delta introduces the unchecked admission. The limit recorded in `SELF-184-42` is therefore an unresolved sibling of finding 5.

2. **[P2] A passthrough forwarder loses guarantees despite proven nonempty input.**  
   [lineage.py:1022](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:1022).

   Amendment 3 §8 requires deriving children **“in dependency order”** and states: **“Waited successful completion may establish later-parent guarantees.”**

   Concrete input:

   ```text
   Scheduled Parent:
     Branch leg 1: call MID(wait=true, abort_on_error=true)
     Branch leg 2: read DPP K → stop

   MID:
     passthrough → call WRITER(wait=true, abort_on_error=true)

   WRITER:
     No Data → Decision, both arms set_dpp K → stop
   ```

   MID’s entry carries `provably_nonempty=True`, but its symbolic group has unknown cardinality. `_child_guarantee` requires exactly one document and ignores the existing nonempty proof, so MID exports no guarantee for K.

   Both compiler entry points refuse the parent with `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE` at `/body/steps/0/legs/1/steps/0`; public plan and compile reproduce that refusal. Inlining WRITER’s body into MID passes. Consulting the existing nonempty proof also makes the forwarding composition pass while retaining both flag checks. The singleton requirement narrows the planned execution-proof condition.

Evaluation-1 dispositions:

- `ARCH-184-r1-01`: fixed; scoped refutation accepted for both literal forms.
- `ARCH-184-r1-02`: fixed.
- `ARCH-184-r1-03`: fixed.
- `ARCH-184-r1-04`: fixed.
- `ARCH-184-r1-05`: not fixed across required siblings — finding 1.
- `ARCH-184-r1-06`: not fixed across forwarding calls — finding 2.
- `ARCH-184-r1-07`: fixed.
- `ARCH-184-r1-08`: fixed.
- `ARCH-184-r1-09`: fixed.

Revised correction rows:

- `C5a`: accepted.
- `C7a`: accepted.
- `C13a`: accepted.
- `C14a`: accepted.
- `C20a`: rejected as fully realized — finding 2.
- `C21a`: rejected as fully realized — finding 1; its interposition correction is accepted.

The OPEN N×M boundary, caller-cohort overlay restriction, repeated No Data cache-mutation refusal, stable DPP establishment rule, scheduled legacy placement, and D2/D4 exemptions are accepted. Waiting, aborting on error, and proving execution are valid guarantee conditions; finding 2 concerns the additional singleton restriction.

No additional regression was found in C1–C4, C6, C8–C12, C15–C19, C22–C25 or either owner decision. **474 targeted tests passed**, alongside the additional probes above. HEAD remained `bb739a8033fb4bd16f5640fd51e621ee85c5f4c0`; no files were changed.

VERDICT: ISSUES FOUND

