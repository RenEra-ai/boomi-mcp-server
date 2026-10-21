I’ll read the ledger dispositions first, then trace the correction delta and its evidence. I’ll keep this read-only and make no MCP calls.The tree is clean at `cdf2cef`; the delta contains batches 6 and 7 plus their validation records. I’m using the local architect conformance review skill and splitting the review across cache-proof propagation, caller-dependent DDP judgments, and the forwarding/route witnesses.The cache correction now uses the retrieve’s proof rule for caller judgments and carries the child’s own pre-retrieve content in the contract row. QA r5 records the repaired mixture cases and their refusing controls. I’m checking forwarding and called-root deferral, where that proof could still be lost.Two possible regressions need a closer trace: the aggregate cache check may treat an inspectable child’s unknown-content marker as evidence that retrieval proves no profile, and the new DDP helper may merge same-name observations at a forwarding call. The coverage review also found that control-body cases are still filtered out of the forwarding matrix.The recorded evaluation-2 scenarios are repaired, but two source traces show new regressions: a sibling cache writer can hide a profile mismatch, and combined `drops`/`fresh` reads can produce an invalid contract relation and raise `ValidationError`. The legacy/full-verdict and stored-profile route gaps are addressed; e1-06 remains incomplete because the forwarding matrix still omits control-body cells.Reviewed files: `docs/architecture/{ISSUE_186_AUDIT_LEDGER,PROCESS_IR_V1,COMPLETION_WORKFLOW_RULES}.md`; correction-review archives r7/r8; QA r5 report and provenance records; `authoring/{process_ir_effects,process_ir_projection,contract}.py`; `semantic_validation/{lineage,contracts,context}.py`; affected child-state, component-identity and revision-coverage tests.

Reviewed `71c046f..cdf2cef`. The tree is clean. No edits, tests, MCP calls or network requests were made. Findings below follow from the source paths.

| Finding | Disposition |
|---|---|
| ARCH-186-e2-01 | **RESOLVED** for the reported mixtures: the retrieve and inherited write now share `_proved_profile`, including the child’s own content. |
| ARCH-186-e2-02 | **RESOLVED**: reliance is measured against an unprofiled seed, so destroying proof alone no longer promotes an exact requirement. |
| ARCH-186-e2-03 | **RESOLVED** for the reported local-writer scenario, directly and through a forwarding middle. |
| ARCH-186-e2-04 | **RESOLVED** for the legacy-root and filtered-verdict defects. Broader control-body coverage remains incomplete below. |
| ARCH-186-e2-05 | **RESOLVED**: stored-profile passthrough witnesses cover typed plan, compile and raw apply, directly and through a middle. |
| ARCH-186-e1-04 | **RESOLVED**, as e2-03. |
| ARCH-186-e1-06 | **NOT RESOLVED**: admitted control-body forwarding cells remain omitted. |
| ARCH-186-e1-07 | **RESOLVED**: the new passthrough witnesses complete its remaining route gap. |
| CDX-186-r7-01 | **RESOLVED**: deferral is restricted to `called ∩ ordered`, preserving a judging boundary. |

[P2] An inspectable sibling cache writer hides a profile mismatch — [lineage.py:3421](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:3421)

Use three No Data roots, with every call waited and `abort_on_error=True`:

- PARENT’s first Branch leg produces P1 documents and stores them in unprofiled SOURCE.
- Its second empty-prefix leg calls WRITER, which also produces P1 and stores it in SOURCE.
- Its third empty-prefix leg calls READER: `cache_get SOURCE → cache_put TARGET(P2)`.

The WRITER call inserts `None` through `_child_may_write_caches`, representing content whose profile the contract does not export. SOURCE therefore becomes `{P1,None}`. The new `_completable(together)` guard skips READER’s obligation, admitting the composition.

The faithful flattened twin stores P1 twice, retrieves proved P1 and refuses `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`. The previous per-identity check also refused this scenario.

Preserve the actual content identities of inspectable child writes, or distinguish unexported content uncertainty from documents proved unprofiled. Add sibling/grandchild writer differentials; treating every `None` as a refusal would reopen the genuine-mixture defect.

[P2] Caller-judged forwarding merges relations into an invalid contract value — [process_ir_effects.py:1443](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:1443)

A No Data parent calls passthrough M without X. M writes X on one Branch leg and calls passthrough C on another empty-prefix leg. C has no X writers: one leg runs custom scripting then a non-default Decision on X; another calls a No Data grandchild reading X. C exports both `drops` and `fresh`.

At M’s call, both reads initially produce SCOPE. Seeding entry X changes the dropped read to BEFORE while the fresh read remains SCOPE, marking their shared `(pointer,X)` caller-judged.

The routed classifications lose their original relations and emit hints `drops` and `entry`. The dictionary at line 1443 merges them; sorted observations leave `entry` as the winner. Derivation exports `(X,"entry")`, and `ChildEntryContractV1` raises `ValidationError` because only `inherits`, `drops` and `fresh` are permitted.

The faithful no-X twin returns `PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID` without an exception. Preserve observation identity and original relation throughout routing, exporting every applicable valid relation.

[P2] The forwarding matrix still discards control-body neighbours — [test_issue_184_child_state_transfer.py:10881](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_184_child_state_transfer.py:10881)

The matrix retains `_b30_between` results only `if steps` and discards `_terminal`. Decision, Branch and Try/Catch wrappers return empty steps with the read inside their terminal, so these candidates disappear without measurement or an explicit refusal disposition.

One constructible missing cell is scheduled PARENT calling MID, whose root static Decision calls READER in its true arm; READER’s root non-default Decision reads X, with no writer. Its faithful twin nests the read Decision inside the static Decision, within the nesting bound. Both should admit with empty complete code sets, but the matrix generates neither verdict.

Retain control terminals, choose admitted containing-root forms, and record unsupported pairings with their actual refusal and context.

VERDICT: ISSUES FOUND

