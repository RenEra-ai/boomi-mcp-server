I’m treating this as a plan-conformance audit, not another code-quality pass. I’ll first reconcile evaluation 1 and the ledger, then trace grammar, effect authority/lineage, and served-contract coverage in parallel against the current `c41bcf…HEAD` delta.Evaluation 1’s nine findings are all accounted for in the ledger. I’m now testing the rewritten subprocess path as shipped—not relying on the dispositions—because that is the highest-risk architectural correction and the main new surface since the prior gate.The core trust boundary is server-owned as intended, but I’ve found two important reachability failures that the existing tests do not exercise: canonical aliases do not bind by component identity, and the exposed lineage state cannot correctly summarize every normal exit. I’m pinning both to current lines and checking whether their documented contract language overstates what ships.The lineage rewrite fixed the earlier root-only scan, but it did not complete the planned subprocess contract. A child that genuinely requires input state is still validated independently from an empty state and rejected, and Try/Catch exits discard guarantees made on both normal paths. So the caller-facing `required_reads`/`must_writes` channel is only partially reachable despite the clean commit review.One important negative result: I found no route where caller-supplied map, script, or subprocess content is accepted without server corroboration. The external-writer exception is also correctly bounded and never creates a cache write. The failures are false rejection, incomplete derivation/composition, and served-contract drift—not a caller-controlled fail-open.The implementation is not yet plan-faithful. The grammar half is largely correct and the trust boundary remains fail-closed, but I found nine actionable gaps—several are incomplete closures of evaluation 1 rather than re-reported findings.

## Findings

1. **[P1, items 7–8] Subprocess effects are not compositional end to end.**

   - Every authored root is independently validated from empty state at [workflow.py:1362](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:1362). Consequently, a child with a genuine required read fails on the child itself even when its parent establishes that read before `process_call`. The planned `required_reads` channel therefore has no valid public authored-child path.
   - Try/Catch visits both bodies but returns the pre-scope state at [lineage.py:641](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:641). A child whose try and catch both write `dpp:BOTH` derives no writes. Likewise, Branch continuation semantics discard DDP writes even when every leg writes the same key.
   - Subprocess derivation invokes the walk without the child’s verified capabilities at [process_ir_effects.py:681](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:681). Thus a child-local external-writer declaration validates the child but is not reflected in its parent-facing summary.
   - The whole `data_process` semantic kind is excluded at [process_ir_effects.py:537](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:537), so even pure `split_documents`/`combine_documents` children are inert, contradicting the served wording that identifies only scripted data processes as uninspectable.

   The dependency direction—authoring consuming compiler analysis—is correct. The abstraction is not: `walk_lineage.established_at_exit` is validator continuation state, not a normal-exit process summary. The structural remedy is the plan’s compiler-owned summary API, accepting child-local capabilities, collecting every normal exit, and exposing explicit preconditions.

2. **[P1, items 7–8] Recipe-composed subprocesses are missing from the symbol table.**

   [engine.py:1741](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/engine.py:1741) builds symbols from components only. It omits `process_keys=[key for key, _ in composed.process_roots]`, despite [build_symbol_table](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/materialization.py:120) supporting exactly that input. A recipe-composed `process_call` therefore fails resolution, and its truthful subprocess declaration is rejected before derivation.

3. **[P2, items 7–8] Binding still uses raw spelling instead of canonical component identity.**

   `_symbol` requires `symbol.ref == ref` at [process_ir_effects.py:169](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:169), while occurrence binding uses raw membership at [process_ir_effects.py:749](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:749). This contradicts the explicit allowance for multiple refs sharing one component ID at [contracts.py:255](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/contracts.py:255).

   Probe: declaration `$ref:MAP`, root occurrence `$ref:MAP_ALIAS`, both resolving to the same component ID, returns `unbound`. Internal rows also retain declaration spelling instead of being rebound per occurrence.

4. **[P2, items 7–8] Map derivation publishes an incomplete effect as exact.**

   `_join_cache_reads` silently drops externally populated cache joins at [process_ir_effects.py:349](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_effects.py:349), and the test explicitly pins an empty exact effect at [test_process_ir_effect_declarations.py:1734](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_effect_declarations.py:1734). The served contract nevertheless says every document-cache join contributes its cache read. If the effect model cannot represent “read, externally satisfied,” the map should be inert rather than trusted with the read omitted.

   Two additional, honestly documented narrowings mean this item is not fully realized: script maps always remain inert, and `FunctionFamily` uses optional `effect_kind` metadata with inferred replay safety rather than the planned required effects plus independent replay flag ([map_function_registry.py:144](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/builders/map_function_registry.py:144)). I do not count those two as hidden trust failures, but they are plan narrowing.

5. **[P2, item 5] The connector-entry invariant is not independently derived.**

   [invariants.py:447](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/invariants.py:447) selects `root_calls[0]` in CFG tuple order; it does not reconcile numeric root source paths with ordering edges as planned. A mutant reversing the two calls’ source paths/semantics while preserving node order and edges passed `check_cfg_invariants`. That is the correlated lowering defect the independent invariant was intended to catch.

6. **[P2, items 7–8 and 10] The served public schema differs materially from the plan.**

   The implementation collapses property and cache references into `ProcessIRStateReferenceV1(scope, name)` at [authoring_workflow.py:302](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/authoring_workflow.py:302). The planned cache payload using `cache_ref` is rejected.

   `ProcessIRScriptLanguageV1` is also a second hard-coded literal at [authoring_workflow.py:350](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/authoring_workflow.py:350); `CustomScriptingOpV1` independently repeats `Literal["groovy2"]` at [process_ir.py:571](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:571), and the alias is absent from both public export lists. This contradicts ledger row CDX-S6-06’s “derived” closure.

7. **[P2, item 6] The generic placement derivation silently drops legal literals.**

   [body_capabilities.py:189](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/body_capabilities.py:189) returns only `literals[:1]`. A synthetic member with `kind: Literal["a", "aa"]` accepts both values, while the derived matrix contains only `"a"`. The supposedly independent test reader repeats the same first-element assumption at [test_process_ir_rich_control_bodies.py:626](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_rich_control_bodies.py:626). PEP 604 unions are also not handled.

8. **[P2, items 3 and 10] Served contract text is duplicated and inaccurate.**

   - Five hand-written `semantic_rule.effect.*` rows remain at [process_ir_projection.py:790](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_projection.py:790), while five additional generated `effect_authority.*` rows are added at [process_ir_projection.py:1645](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_ir_projection.py:1645). The plan required one generated `semantic_rule.effect.*` family, including INERT behavior. The parallel namespace is material over-delivery and restores two trust descriptions to maintain.
   - The subprocess and map summaries overstate the behavior described in findings 1 and 4.
   - Static summaries still describe inert effects as having “no typed contract” and external state as established by the assumption ([findings.py:49](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/findings.py:49), [errors.py:1192](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/errors.py:1192)).
   - A bare target still says it must be followed by `stop` at [process_ir.py:2248](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:2248), although `target + return_documents` now ships.

9. **[P2, items 5–8 and 10] Required acceptance coverage remains missing.**

   Evaluation 1’s integration finding was only partly closed: the new map test directly calls resolver → compiler → emitter at [test_process_ir_effect_declarations.py:1915](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_effect_declarations.py:1915), not `AuthoringRequestV1` plan → compile. Registered-script, subprocess, and external-writer public emission cases remain absent, as do all three planned effect XML goldens.

   The required synthetic additional-body-context witness, model-constructed catch-terminal enforcement witness, and corrupted nested-entry-role mutant are also absent; the nearest tests cover only the current reachable model set and root-role mutations ([test_process_ir_rich_control_bodies.py:648](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_rich_control_bodies.py:648), [test_process_ir_compiler_invariants.py:2159](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_compiler_invariants.py:2159)).

## Item-by-item assessment

| Item | Assessment |
|---|---|
| 1 | Realized. Shared control-step union and connector-scoped restriction match the plan. |
| 2 | Realized. Try terminal is precisely `stop \| return_documents`. |
| 3 | Runtime grammar realized; caller diagnostic diverges. |
| 4 | Realized, including catch-only trailing `cache_put`. |
| 5 | Grammar and lowering realized; independent invariant diverges. |
| 6 | Current ten rows are derived and correct; generic derivation and mandated witnesses are incomplete. |
| 7–8 | Partial. Accepted content remains server-owned and root-local, but canonical identity, recipe reachability, subprocess composition, and parts of map inspection are missing or narrowed. |
| 9 | Realized. `cache_get` is canonical for external writers; `DocumentCacheRetrieveNodeV1` remains unchanged. |
| 10 | Partial. Schema, generated-rule shape, diagnostic text, and acceptance artifacts diverge. |

I found no route where caller-supplied map, script, or subprocess content is trusted without server corroboration. External-writer declarations remain bounded to a warning and establish no cache write. The trust failures are incomplete server derivation and unreachable truthful declarations, not caller-controlled content injection.

Scope checks: HEAD is the stated `02a55449…`, the baseline is an ancestor, and the worktree stayed clean. The local branch is actually `codex/issue-154`, not `codex/issue-177`; that does not affect the reviewed range. A focused test run passed 415 tests, while the targeted counterexamples above exposed gaps outside that coverage. I did not count the filed #179 limitation or the explicitly non-authoritative connector-prefix golden as new findings.

VERDICT: ISSUES FOUND
