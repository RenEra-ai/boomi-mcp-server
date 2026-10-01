I’ll review the correction delta from `bb739a8` to HEAD, reconcile the named ledger rows, and run targeted tests only where needed to verify a claim. I’ll keep the checkout unchanged and use no network or MCP calls.
The checkout is clean on `codex/issue-184` at `fd5ca930`. I’m applying the local architect-conformance review skill to map each requested decision to the current source, ledger evidence, and witnesses.
The prior P1 now carries retrieved-cache provenance through the shared count-preserving authority. I’m checking the obligation sites and re-cache path around that carry. The forwarder’s singleton limit is recorded explicitly; I’ll judge its scoping against the plan’s wording.
The cache obligations now use one sealing predicate, and re-cached cohorts retain their source caches. The cycle loop unions the fail-closed fields and checks that each pass grows or stays equal. I’m now checking the revision corpus and served remediation, including whether the documented bounds match the code.
The Message witnesses pass in my probes. I found a possible remaining sibling: a map can rebuild the stream after retrieval and drop the cache attribution used by a bound path. I’m checking a legacy-dialect case against both compiler routes and an in-process control to establish whether it actually bypasses the check.
The map probe confirms a remaining sibling of the earlier P1: a legacy child drops the cache attribution after retrieval and accepts a bound path that its flattened equivalent refuses. Preserving that attribution in memory restores the refusal and leaves the valid control admitted. I’m now checking the correction witnesses and revision coverage before giving the dispositions.All 114 selected witnesses passed, including the cycle invariant, keyed remediation, and revision-corpus guard. The remaining gap needs the re-cache shape: both roots then compile despite the second cache containing a literal-only path writer. A simpler child keeps its own refusal, which narrows the finding to the missing re-cache obligation.Reviewed files: `.github/workflows/tests.yml`; `Dockerfile`; `scripts/wave_gate.py`; `docs/architecture/{ISSUE_184_AUDIT_LEDGER.md,AUTHORING_WORKFLOW_V1.md,M12_COMPATIBILITY_INVENTORY.md,PROCESS_IR_V1.md,PROCESS_IR_SEMANTIC_VALIDATION_V1.md}`; the archived evaluation-2 `review.md`; `src/boomi_mcp/authoring/{contract.py,process_ir_effects.py,process_ir_projection.py,revision_corpus.py,revision_corpus_v1.json.gz}`; `src/boomi_mcp/compiler/process_ir/diagnostics.py`; `src/boomi_mcp/compiler/process_ir/semantic_validation/{context.py,contracts.py,findings.py,lineage.py}`; `src/boomi_mcp/{errors.py,models/process_ir.py}`; `tests/{_m12_11_support.py,_m12_12_legacy_inventory.py,_revision_corpus.py,conftest.py,test_connector_replay_review_round3.py,test_issue_153_canonical_apply_e2e.py,test_issue_155_contract_id_grammar.py,test_issue_155_discovery.py,test_issue_155_identity_projection.py,test_issue_184_child_entries.py,test_issue_184_child_state_transfer.py,test_issue_184_ddp_invalidation.py,test_issue_184_passthrough_entry.py,test_issue_184_revision_corpus.py,test_issue_184_revision_coverage.py,test_m12_11_authoring_compile.py,test_wave_gate.py}`.

1. **[P1] The map transition still drops a required cached-writer alternative** — [lineage.py:2597](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/semantic_validation/lineage.py:2597).

   Amendment 3 §7 requires: **“A bound path must pass for every possible selected writer. Never discard an inconvenient writer alternative.”**

   The specialized map transition rebuilds `_Stream` without `retrieved_from` or `retrieved_origins`, bypassing batch 20’s carry. The shared count-preserving authority includes `map`.

   Concrete input, using the child-entry tests’ symbol table with `M22.input_profile_ref` and `output_profile_ref` unset:

   ```text
   Parent, scheduled Branch:
     GET → X = static("/clients/") + DPP key(default="")
         → terminal cache_put CACHE
     GET → X = static("/c/1") → terminal cache_put CACHE2
     empty prefix → terminal process_call CACHE_CHILD

   CACHE_CHILD, legacy source(RCONN, GET) → Branch:
     cache_get CACHE → set_dpp Y = DDP X → terminal cache_put CACHE2
     cache_get CACHE2 → map_ref M22 → GET(path_binding=X) → Stop
   ```

   **Both roots validate and compile clean through both compiler entry points.** The child exports CACHE’s property requirements but omits `("$ref:CACHE2", "X", None, True)`. Consequently, the parent’s literal-only writer in CACHE2 escapes validation. The flattened equivalent refuses `PROCESS_IR_SEMANTIC_DYNAMIC_PATH_NO_DYNAMIC_SEGMENT` at `/body/steps/1/legs/3/steps/2/path_binding`.

   Removing X from the parent’s CACHE2 documents likewise leaves both roots admitted, while the flattened graph refuses `PROCESS_IR_SEMANTIC_DYNAMIC_PATH_DDP_NOT_ESTABLISHED`. Restoring only the map’s cache attribution **in memory** restores both refusals at the parent call’s `/process_ref`; the dynamic-writer control remains admitted.

   This also reproduces at `bb739a8`: it is an unresolved sibling within the requested batch-20 coverage. It differs from the recorded fail-closed case with a map **before re-caching**.

ARCH-184-r2-01: **not fixed completely — finding 1**. The Message case and its disclosed rule-8 consequence are correct.

ARCH-184-r2-02: **scoping accepted**. “May establish” does not require replacing the singleton gate with an at-least-one gate; the asynchronous and tolerated-failure restrictions remain enforced.

C20a: **accepted under that scoping**.

C21a: **rejected as fully realized — finding 1**.

SELF-184-42a: withdrawal of the accepted residue is justified; complete closure remains unsupported by finding 1.

- **4(a): accepted** — one sealing authority governs the obligation sites.
- **4(b): accepted** — a child’s own append preserves caller obligations.
- **4(c): accepted** — cohort clearing requires whole-run or valid Branch-relative execution proof.
- **4(d): accepted** — re-cached cohorts carry their source caches, within the recorded bounds.
- **4(e): accepted** — failed binding checks record obligations through all three sites.
- **4(f): accepted** — the dedicated refusal, child-owned required set, and cleanup exemption conditions implement rule 8.
- **4(g): accepted** — cycle obligations propagate to a fixed point as defence in depth.
- **4(h): accepted** — §7 supports treating these child writes as unknown possibilities.
- **4(i): accepted** — runtime corpus replay and its coverage guard realize §10 within the stated finite-input and returned-verdict bounds.
- **5: accepted** — distinct document requirements retain unknown consumption; union carries preserve fail-closed fields; the checked invariant replaces the inadequate pass count. Cause evidence, keyed answers, placement discrimination, and the corrected citation match their authorities.

For the recorded limits, I find no plan sentence requiring recursive default-taint analysis, additional admission across the disclosed pre-re-cache map/connector gap, inherited-removal visibility through cycles, relaxation of the unknown cycle seed, or removal of the unreachable predecessor messages. Those scopings are accepted. Finding 1 concerns a separately reproduced **fail-open** map case.

No additional regression was found in the previously accepted corrections, evaluation-1 fixes, or either owner decision.

Reviewed `bb739a8..fd5ca930`. **114 targeted tests passed**, alongside the finding’s local probes. The checkout remains unchanged.

VERDICT: ISSUES FOUND

