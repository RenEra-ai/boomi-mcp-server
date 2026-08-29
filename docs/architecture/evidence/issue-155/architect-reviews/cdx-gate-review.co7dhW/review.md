I’ll inspect the `eb22351..HEAD` delta against Slice C’s governing Unit 5 contract, then independently trace acceptance coverage and the two known scope additions. I’ll keep later-slice D–F absences out of findings and report only plan-conformance defects with exact code and plan anchors.I’m applying the repository’s stored architect-review discipline because this is the same bounded issue-155 gate: inspect the real delta, anchor every accepted finding to both code and the controlling plan clause, and separate Slice C defects from intentionally absent later units.I’ve narrowed the review to two architectural questions: whether the snapshot is truly the single authority at every compile sink, and whether it carries the identity facts Slice D/E were promised. I’m now validating those against the actual call graph and the acceptance tests.The focused suite is green (124 targeted tests), but the plan-versus-tree check is exposing gaps those tests deliberately do not cover. In particular, one test explicitly records the canonical comparison as tautological and another records the required no-client diagnostic as absent.The implementation has several sound pieces—the compiler refusal, normalized projection, shared reuse predicate, and constructor-based rebinding—but they do not add up to the plan’s single trusted snapshot. I’ve confirmed two concrete fail-open probes: a typed GET assertion can still compile submitted POST XML as GET, and unrelated XML fields can satisfy the current document-wide verb/path reader.Slice C does not conform to §5.C and should not clear the architect gate. The compiler refusal and several supporting mechanisms are sound, but the trusted authority boundary is incomplete.

## Findings

1. **Caller `connector_metadata` remains authoritative in typed and recipe compilation.** §5.C requires the snapshot before normalization, threading through `run_recipes`, and assertion-only metadata at every sink ([plan §5.C](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/plans/issue-155-strategy.md:252)). Instead:

   - Planning normalizes first ([workflow.py:1808](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:1808)).
   - Recipe normalization calls `run_recipes` without a snapshot ([workflow.py:523](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:523)); `run_recipes` has no snapshot parameter ([engine.py:1574](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/engine.py:1574)).
   - Recipe compilation builds its symbol table without a snapshot ([engine.py:1741](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/engine.py:1741)).
   - `build_symbol_table` copies caller metadata directly into `connector_type` and `action_type`; the snapshot supplies only `requires_path_binding` ([materialization.py:193](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/materialization.py:193)).
   - Only the raw integration helper calls `assert_declared_matches_resolved` ([integration_builder.py:7771](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:7771)).

   A direct typed-workflow probe with declared GET and submitted POST XML returned `valid=True`, no diagnostic codes, and `symbol_action='GET'`. That is the override §5.C was meant to remove, and it leaves AC8n and the read-only-no-bypass arm unresolved.

2. **The XML authority reads the whole document rather than the operation’s configuration, and it cannot derive non-REST actions.** The parser collects every `customOperationType` and every `field id="path"` without tracking element scope ([snapshot.py:203](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:203)), then derives action and route from those document-wide sets ([snapshot.py:267](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:267)). Its verb rule is explicitly REST-only ([snapshot.py:430](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:430)), while the assertion skips any unresolved field ([snapshot.py:578](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:578)).

   Direct probes confirmed:

   - A REST `<operation/>` plus unrelated `<Other customOperationType="GET"/>` resolves as GET.
   - An unrelated static `field id="path"` makes an operation with no scoped path resolve `route_state="static"`.
   - Database `<DatabaseSendAction/>` declared as Get is accepted.
   - SOAP XML with an arbitrary declaration is accepted because `operationType` is never read.

   Raw create and update use this parser ([snapshot.py:381](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:381)). Consequently both the identity assertion and blank-path cross-check remain bypassable.

3. **The planned trusted snapshot model was not built.** Unit 5 requires mode and authority types, actual IDs and versions, configuration digests, extension state, strict account scope, operation/connection separation, collision decisions, and root projection ([architect addendum](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/plans/issue-155-architect-addendum.md:311)). The implemented identity contains only key, family/action, route facts, parser flags, and an open `source` string ([snapshot.py:79](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:79)); the snapshot itself contains only `identities` and `lookup` ([snapshot.py:146](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:146)). There is no `for_root`.

   `component_get_xml` returns actual ID and version ([components/_shared.py:235](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/_shared.py:235)), but the live collector keeps only its XML ([integration_builder.py:7711](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:7711)). `_PlanInternals` retains neither the snapshot nor root-specific tables ([workflow.py:1776](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:1776)). AC8j is therefore missing, not merely deferred.

4. **Unreadable reuse fails open beyond the plan’s narrow no-client limitation.** Any live-read exception is swallowed ([integration_builder.py:7710](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:7710)); the reuse becomes all-unknown ([snapshot.py:497](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/connector_resolution_snapshot.py:497)); that maps to `requires_path_binding=None` ([materialization.py:120](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/materialization.py:120)); and the compiler refuses only explicit `True` ([connector_resolution.py:681](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/connector_resolution.py:681)). An unbound reused-operation probe consequently compiled successfully.

   The narrow no-client/reference-only S11 limitation is represented, but D6′ also requires `CONNECTOR_REPLAY_IDENTITY_UNAVAILABLE` for a retry-bearing reused operation. The implementation explicitly records that this diagnostic and consumer do not exist ([errors.py:536](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/errors.py:536), [test:881](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_155_identity_projection.py:881)).

## Plan-conformance summary

| Artifact | Judgment |
|---|---|
| Snapshot module and two named models | Names exist, but the required trusted identity/account/lifecycle contract does not. Drift. |
| `normalized_identity_projection` | Substantially as designed: credential-free, pure, avoids `builder.build`, and handles extension-bound routes conservatively ([connector_builder.py:311](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/builders/connector_builder.py:311)). |
| Snapshot through `build_symbol_table` | Optional and partial; absent from recipe compilation and not used to assert metadata inside the builder. Drift. |
| `ComponentSymbolV1.requires_path_binding` | Field and mapping exist. Raw create/update and successful reference/collision readbacks work, but unreadable reuse and unscoped XML remain fail-open. |
| Compiler refusal | Built as described and load-bearing. |
| Two reconstruction helpers | Built differently, justifiably. `SymbolTableV1.rebinding` re-enters the validated constructor because `model_copy` skips validators and `extra="forbid"` ([contracts.py:311](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/contracts.py:311)). Both helpers use it, and future table fields are preserved automatically. |

## Scope

- The import-order test is in scope. It is affected QA for a circular import introduced by Slice C and validates served modules in fresh interpreters with a non-vacuity control ([test_issue_155_import_order.py:1](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_155_import_order.py:1)).
- The maintained M12.12 rebaseline script and its dedicated test module are useful and well-tested, but they are generic tooling scope, not a §5.C artifact. The plan prescribes the existing `_m12_12_legacy_inventory.py` commands ([strategy §10](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/plans/issue-155-strategy.md:385)). Keeping the new producer under #155 needs an owner amendment like the archiver received, or it should move to a tooling issue.

## Coherence and later-slice impact

The precedence chain, shared reuse predicate, REST-scoped verb rule, and separate “contradicted” versus “blank present” signals are locally coherent. The two signals genuinely answer different questions and should remain separate.

The resulting Slice C architecture, however, is a sequence of locally sound repairs around the raw/apply path—not the plan’s single trusted snapshot. That distinction matters:

- D cannot consume a root-projected snapshot with account, mode, ID/version, digest, and route pins; it must first redesign C’s models and thread them through recipes.
- E has no retained identity snapshot to recheck and would otherwise reread facts that C was supposed to pin once.
- F lacks the strict account/digest identity needed to relate evidence safely.

Those are consequences of missing Slice C deliverables, not findings that D, E, or F themselves are absent.

Focused validation: 127 targeted tests passed, including identity projection, import-order, rebinding, and rebaseline-tool tests. The fail-open probes above still reproduced on clean HEAD `99305353501adbbed1d39214ebb703da094c6ce3`.

VERDICT: ISSUES FOUND

