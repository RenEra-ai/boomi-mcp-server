I’m treating this as the bounded §6 architect gate, not another general diff review. I’ll inspect `d04a248..HEAD`, trace the five promised artifacts and every production authorization surface, then separate landing blockers from acceptable residue. I’m also using the repository’s prior architect-review memory workflow because it matches this exact issue family and enforces the requested plan-clause/file-line evidence discipline.Baseline is clean at `dede3ad7403ba98657eec1a092c7472460df2bf0`; the review range contains the twelve slice-D commits listed above. I’m splitting the audit into three independent traces—artifact conformance, authorization reachability, and provenance/landing state—while I read the complete delta and run the adversarial checks myself.A potentially consequential deviation has emerged: the production symbol table uses relocatable placeholder component IDs, while registry operation records carry observed component identities. I’m tracing whether the omitted trusted snapshot is therefore not just a signature change, but the only bridge that could make a real positive grant possible after slice F.The repository’s tracked plan confirms the signature difference was architectural, not cosmetic: it names a per-root `build_symbol_table(..., snapshot, registry)` and an offline matching-record/reused-live-identity acceptance arm. I’m now testing that exact end-to-end construction, because the shipped tests currently prove only a hand-built compiler table can mint.The focused run initially failed at collection because this checkout does not install `src/` into the active venv by default; that is an invocation issue, not a test result. I’m rerunning with the repository’s expected `PYTHONPATH=.:src`, then I’ll exercise the rootless-versus-projected compiler behavior directly.The central mechanism is not surviving the adversarial path trace: projection enforces the grant, but rootless compiler/materialization paths still revert to the old contract-pair authority. I’m now separating that from the independent positive-path problem—real snapshot identities never reach the minter—so the landing judgment names two distinct defects rather than one vague wiring concern.The artifact accounting is now complete. Most of the five batches are faithful, including the schema workaround; the two known signature deviations are not equivalent: splitting lowering from minting is reasonable, but dropping the snapshot (and never constructing production contract symbols) makes the positive grant path vacuous. I’m doing the final landing/ledger reconciliation now.Issues found. Most named artifacts conform, but the two central invariants—production minting and mandatory consumption—do not.

## Blocking findings

1. **Critical — the per-call grant gate remains optional on rootless tables.**

   Section 5.D requires the retry consumer to operate on a root-projected table and use `(contract_ref, operation_ref, call_source_path)` ([plan §5.D](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/plans/issue-155-strategy.md:262)). The triple lookup itself is correct, but the consumer sets `grants=None` when `process_root_ref` is absent and then skips the lookup entirely ([error_handling.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/error_handling.py:285), [error_handling.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/error_handling.py:378)).

   Rootless tables still reach real compile sinks:

   - Authoring plan validation projects correctly, but later artifact compilation uses the rootless base table ([workflow.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:1494), [workflow.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:2257)).
   - Recipe compilation projects correctly ([engine.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/engine.py:1850)).
   - Raw planning builds rootless symbols and passes them directly to materialization ([integration_builder.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:7802), [integration_builder.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:7824)).
   - Apply rebuilds another rootless table and recompiles against it ([integration_builder.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:8143), [canonical_process_apply.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/canonical_process_apply.py:239)).

   Direct probes confirmed all three public compiler entry points accept the conditional retry with a matching operation contract and zero grants; the same input under a projected empty-grant table refuses with `PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING`. A production-helper probe also built a materialization plan and emitted XML rootless.

   The reachability test does not prove otherwise: it scans each whole module for the string `project_grants_for_root(`, without checking the actual compile sinks ([test_issue_155_discovery.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_155_discovery.py:430)).

   The empty registry currently masks this. Once the positive-path defect below is repaired, raw/materialization/apply paths regain operation-pair authority without requiring the per-call grant.

2. **Critical — the trusted snapshot is absent from the minting authority.**

   Splitting lowering into `project_grants_for_root(root_ir, …)` and minting into `mint_idempotency_grants(cfg, …)` is justified. Dropping the snapshot is not.

   `build_symbol_table` receives a snapshot, but uses it only for `requires_path_binding`; component identity still comes from its resolver ([materialization.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/materialization.py:142), [materialization.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/materialization.py:218)). The default resolver creates `id-KEY` placeholders. Projection receives only that table and the registry ([connector_resolution.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/connector_resolution.py:893)).

   Corroboration then compares record identities against those symbol-table IDs, not the independent snapshot ([connector_resolution.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/connector_resolution.py:847)). The positive tests hide this by deriving the record’s expected IDs from the same synthetic symbol index ([test_process_ir_error_handling.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_process_ir_error_handling.py:1913)).

   The stated “compiler has no live reading” rationale is incomplete: the prescribed snapshot is precisely the independent input, and it can carry component IDs, versions, account and configuration facts when the route has them ([connector_resolution_snapshot.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:103)). Unknown fields should remain fail-closed; they should not be replaced by the evidence record’s own assertions.

3. **Standard — no production path constructs idempotency contract symbols.**

   `build_symbol_table` returns only component symbols ([materialization.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/materialization.py:260)). A source-tree search found no production construction of `IdempotencyContractSymbolV1`; only its model declaration and test fixtures exist.

   Therefore slice F adding genuine operation records will not, by itself, make a candidate usable. The caller can discover and author the correct `$ref:KEY`, yet planning still returns `PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING`. This is additional capability-reachability failure beyond the deliberately empty packaged registry.

## A. Artifact conformance

| Artifact | Judgment |
|---|---|
| `IdempotencyContractSymbolV1.record_digest` | **Built.** Optional representation is acceptable because minting refuses a missing digest ([contracts.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/contracts.py:237)). |
| Pair uniqueness, sort and index | **Built as planned.** Contracts are keyed consistently by `(ref, operation_ref)` ([contracts.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/contracts.py:374)). |
| `_require_resolvable` and `key_reference` consumers | **Built as planned.** Both use the pair key. |
| `process_root_ref`, grant symbol and grant index | **Shape built.** Reconstruction uses derived-field rebinding, so grants survive both helpers. Enforcement is incomplete because rootless compilation disables it. |
| Triple grant lookup | **Built exactly**, but only on projected tables. |
| Minter signature/factoring | **Built differently.** Separating lowering from CFG minting is justified. Omitting the snapshot and registry-driven contract population is material drift. |
| Shared `$ref:KEY` grammar | **Built differently and justified.** `ids.py` owns the constant and predicate ([ids.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/connector_replay/ids.py:159)); the same constant is served as schema metadata beside the named validator ([process_ir.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/process_ir.py:660)). Given measured Pydantic 2.12.3 behavior, this is better conformance than the literal `StringConstraints` prescription. |
| Discovery action | **Built.** It has a closed candidate field set, empty-success behavior and its own unavailable diagnostic ([discovery.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/connector_replay/discovery.py:27), [query_components.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/query_components.py:415)). |
| `connector_replay_semantics` fingerprint row | **Built and account-independent.** Only class semantics enter the revision; operation records do not ([contract.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/contract.py:894), [test_issue_155_discovery.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_155_discovery.py:118)). |

## B. Is the gate load-bearing?

No.

Recipe compilation and authoring validation reach it. Raw planning, materialization, later authoring compilation and apply recompilation do not preserve the projected table. The old contract-pair authorization remains active anywhere `process_root_ref is None`.

There is no presently exploitable public positive because production contract symbols and records are absent. That is masking, not a refutation of the structural bypass.

## C. Provenance judgment

The chain is stronger than digest-only corroboration: contract, operation, connection, portable family and action all have to agree.

The intended split can be sound only if:

1. Minting correlates the record with an independent snapshot.
2. The grant retains an immutable record pointer.
3. Slice E reloads that exact record and compares account, versions, configuration and route coverage against fresh live readings before mutation.

`record_digest` supplies the immutable pointer, so E can theoretically close the later freshness gap. But current D omits step 1 and loses projected/consumed-grant context on several paths. E would therefore need to reconstruct the authority tuple, not merely “recheck the grant.”

## D. What E inherits

E inherits more than the plan assumed:

- It has no reliable root-projected symbol table at raw/apply compilation.
- `_PlanInternals` retains the rootless table rather than per-root consumed grants.
- It must create the production contract-symbol path D omitted.
- It must recover snapshot-derived identity independently before trusting `record_digest`.
- The minter currently grants every call carrying `key_reference`, including retry-zero calls. A probe with `retry_count=0` minted one grant. That is inert in D because the retry consumer never consults it, but E must not interpret every minted grant as “consumed evidence.” Either narrow minting to positive retry regions or derive E’s attestation set from actual retry consumption.

I would accept that last point as explicitly recorded E debt because it authorizes no replay today; I would not let E attest or reread from the unfiltered tuple.

## E. Landing and residue

I would refuse landing until findings 1–3 are corrected and validated.

I would accept as conforming or recorded residue:

- The schema-metadata workaround instead of `StringConstraints`.
- The lowering/projection helper split itself.
- The empty packaged registry and empty semantics contribution.
- The planned absence of slice-E rechecks and slice-F evidence.
- Retry-zero over-minting only with the bounded E acceptance criterion above.

Separately, the landing record needs correction. The final archived commit review attests `7755c55`, while HEAD is `dede3ad`; the latter added the ledger row and review archive. Thus the statement that the review “covers the tree this row is written into” is false ([ledger](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_155_AUDIT_LEDGER.md:1488), [archive index](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/evidence/issue-155/index.jsonl:96)). This is record-only—not a new implementation Critical—but must be made honest. The same row correctly says the architect, wave and closing gates remain owed.

Validation: the focused suite passed `243 passed`; the worktree remained clean. Runtime probes covered rootless versus projected compilation, production materialization/XML emission, placeholder versus actual component identities, and retry-zero grant minting.

VERDICT: ISSUES FOUND

