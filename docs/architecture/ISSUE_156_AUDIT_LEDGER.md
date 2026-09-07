# Audit ledger — issue #156 (M12.18 Notify and recovery subprocess semantics, T4 + T5)

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

Plan of record: `.codex/plans/issue-156.md` — the attested Codex architect plan, attestation
`.codex/plans/issue-156.attest.json` (message sha256
`42f95604645969c3df29ac5cf64fda568b7d7768cc76c17387e4e5b484baaf2d`, thread
`01a074c1-b2d4-7840-ab43-70d3dc139c3f`, turn token 2, gate `architect`, collected
`ok:true`/`stopped:true`). Where the plan and this ledger disagree, the correction is recorded
here with its evidence and the plan is NOT silently re-authored.

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Issue | #156 — M12.18 Notify and recovery subprocess semantics (T4 + T5) |
| Step-0 baseline (`$BASELINE`) | `b07babeb3752d337ffa307c4c486e181e703a2c2` |
| Branch | `codex/issue-156` |
| Branch point | `dev` @ `b07babe` (closing commit of #155) |
| Baseline suite | 11555 passed, 19 skipped (11574 collected) — full non-KB, local `.venv` 3.12, `PYTHONPATH=src`, before any edit |
| Baseline manifests | 11574 required nodes (`tests/fixtures/wave_gate/test_nodes.jsonl`), 74 active goldens (`tests/fixtures/wave_gate/goldens.jsonl`) |
| Slice kind | behaviour-affecting (new canonical vocabulary, new lowering, changed served contract text) |

The collected count 11574 equals the recorded floor, so the floor is current on the branch point.

### Environment correction recorded at step 0

The Codex gates could not run at first: `~/.codex/config.toml` pins `model = "gpt-6-astra"`, and the
installed `@openai/codex` 0.151.0 rejected every turn with API 400 *"The 'gpt-6-astra' model
requires a newer version of Codex."* The first architect round was collected `--outcome failed`
(`declared_failed`, daemon confirmed stopped) and produced no plan. With owner approval the global
CLI was upgraded to 0.153.4 and the gate was re-run from a fresh session and run directory. No repo
file was involved; recorded here because a failed-then-rerun gate round must be visible in the audit
record rather than inferred from the surviving one.

## Loop roster (enumerated in advance, before the first correction)

Per `CLAUDE.md` the roster is fixed before any correction is applied. A gate not on this list cannot
mint a loop mid-run; adding one is itself a recorded checkpoint decision.

| # | Logical loop | Authority | Scope |
| --- | --- | --- | --- |
| 1 | Stage-1 QA | `boomi-qa-tester`, live through the public MCP tool boundary | the slice's affected scenarios |
| 2 | Stage-2 repo commit review | detached Codex review per `CLAUDE.md` §5b–5e | initial: `--base $BASELINE`; then each fix delta |
| 3 | Architect implementation review | `/codex-issue` §6 gate (`--gate review`) | implementation vs `.codex/plans/issue-156.md`; **capped at 3 evaluations** by `docs/architecture/COMPLETION_WORKFLOW_RULES.md` |
| 4 | Composite wave gate | `scripts/wave_gate.py` | full suite, golden manifest, determinism, wave-delta review, one live scenario per changed capability class |
| 5 | Terminal correction loop | only if a final non-blocking batch mutates the tree | that batch |

| 6 | Owner-requested architect consultation | `/codex-issue` §3 gate (`--gate architect`) | ADDED to the roster on 2026-09-07, after the slice had landed, by owner request. A gate not on the roster cannot mint a loop mid-run, so the addition is itself a recorded checkpoint decision (checkpoint 4 below) and this loop inherits the originating loop's cumulative history. It is NOT a fourth evaluation of loop 3: that loop's authority is implementation-versus-plan and it is closed by the three-evaluation cap. This one answers one closed question — where the deferred row goes — and its findings are dispositioned like any other gate's. |

Pre-implementation live-oracle capture (architect plan §1 step 2 / §5) is **evidence provisioning**,
not a QA evaluation: it runs at the baseline tree with no source change and debits no loop.

## Plan corrections (recorded before the first correction is applied)

Claude's implementation plan is `.codex/plans/issue-156.claude.md`. It verified the architect plan
against the tree at `b07babe` and the following corrections OVERRIDE the architect plan. Each was
re-verified in the main thread before adoption; the architect plan is not silently re-authored.

| # | Architect said | Actually | Evidence (re-verified) |
| --- | --- | --- | --- |
| P1 | Retire `PROCESS_NOTIFY_CONFIG_INVALID` — it is "an advertised identity with no canonical producer" | **False premise.** It has live producers in `process_flow_builder.py` and is pinned by 14 assertions across `test_process_flow_builder_trycatch_dlq.py` (9), `test_process_flow_builder.py` (3), `test_integration_builder.py` (1), `test_schema_template_process_flow.py` (1). It is absent from `ERROR_TAXONOMY` because it is a LEGACY builder code, not a canonical one. **Keep it on the legacy path**; map only the canonical path to `PROCESS_IR_SCHEMA_INVALID` / `PROCESS_IR_SCHEMA_UNKNOWN_FIELD` / `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY`. Ship a producer-coverage test in place of the retirement. | `grep -n PROCESS_NOTIFY_CONFIG_INVALID tests/*.py src/boomi_mcp/errors.py` |
| P2 | Add the new semantic code to `semantic_validation/validation_policy.py`'s "closed diagnostic universe" | That module is a legacy-adapter **exemption registry** (`_EXEMPT_CODE`, `_POLICY_REGISTRY`). There is no closed universe there; a new code is non-demotable **by absence**. No edit; a negative test asserts it is not in `_EXEMPT_CODE`. | module read |
| P3 | Rework `lower_cfg_to_emission_plan` and `finalize_edges` for main-spine-first ordering | Both already produce the required order once CFG node order is right (`finalize_edges` sorts by `(source ordinal, local_ordinal)`; the plan assigns ordinals by `for node in cfg.nodes`). No edit. | `lowering.py` read |
| P4 | Rework `models/process_ir._walk_controls` for the recovery exemption | Its `try_catch` early return stays correct for the ancestor half; the exemption is needed and enforced only at the `body_capabilities` independent enforcement point. | `models/process_ir.py:3428` |
| P5 | Add Notify dispatch arms to `semantic_validation/effects.py` | `_replay_hazard` falls through to `None` for unknown kinds — Notify is neutral without code. Verify by test, do not add arms. | `effects.py` read |
| P6 | _(omission)_ | `_DISCRIMINATOR_TAGS` (`models/process_ir.py:2993`) is a hand-listed frozenset and must gain `"notify"` (and `"continue"`); the architect names only `_NODE_FACTS`. `_node_entries` raises `KeyError` for any `ProcessNodeV1` kind lacking a `_NODE_FACTS` row, so the projection hard-fails until both rows exist. | `process_ir_projection.py:279`, `models/process_ir.py:2993` |

## Scope decision — `ContinueNodeV1`

**IN SCOPE.** Reproducing `golden-000005`
(`connector_scoped_trycatch_notify_dlq_document_cache.xml`) canonically requires an explicit
no-emission try terminal: its region 1 protected path has **no closing shape at all** — it continues
into `shape4 map` — and every existing terminal (`stop`, `return_documents`) emits a shape. There is
no smaller design that reproduces those frozen bytes.

The decision is taken from the issue's acceptance criteria, not by preference: *"both legacy notify
goldens are reproduced byte-identically from canonical IR"* and *"Two sequential connector-scoped
regions compile with correct ordinals; mutation checks prove the updated plan invariants still
detect semantic drift"* are both explicit criteria, and in-scope item 5 names the region derivation,
ordinal allocation and plan-invariant work directly.

Two invariants block it today and are the critical path, both re-verified in the main thread:

* `invariants._check_control_depth` propagates the **incremented** depth to successors and treats
  `try_catch` as a control kind, while the ProcessIR v1 control-depth bound is 2 — so two sequential
  regions sit exactly on the bound and three fail. It must count ACTIVE nesting. (The bound's
  constant is named in `models/process_ir.py`; it is deliberately not spelled here, because the
  ledger scanner reads a code-shaped token as a diagnostic identity and that constant is not one.)
* `invariants._check_region_containment` requires a try edge's target `source_path` to start with
  `<tc>/try_body/` and recursively keeps the whole region inside it — an authorized continuation to
  `/body/steps/N` escapes.

## Findings ledger

One row per raw finding. Columns: source ID · verbatim summary · source gate / run dir /
attestation · original severity label · blocking class · defect class · derived tier + anchor ·
affected SHA/delta · disposition.

| ID | Summary (verbatim) | Source / run dir | Orig. label | Blocking class | Defect class | Tier (anchor) | Affected SHA | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| QA-156-r0-01 | "Your premise 'no source has changed yet' was true at my first command and false 16 seconds later — #156 implementation began editing the same worktree, including `process_flow_builder.py` at 03:52:02Z, 102 seconds after I emitted the oracle at 03:50:20Z. Had I run a bit later, the 'frozen legacy builder' oracle would silently have been the half-implemented one, with nothing failing." | `boomi-qa-tester`, pre-implementation oracle capture (reported as `QA-156-oracle-r1-01`); `agents/reports/2026-09-05-issue-156-oracle-r1.md` | High | QA fixture provenance | dispatch-time-race (the capture ran against the shared worktree instead of an extraction) | **Standard** (no critical anchor: not secrets/security, data loss or mutation accounting, and the source label is High, not P0/P1/Critical) | `b07babe` (tree at capture) | `fixed` — the agent re-emitted all five artifacts from a pristine `git archive b07babe` extraction and got BYTE-IDENTICAL results, so the oracle is proven causally independent rather than assumed. The process defect is real and is recorded as a lesson: a pre-implementation capture must run from an extraction, not the shared tree. |
| QA-156-r0-02 | (reported as `QA-156-oracle-r1-02`) "The freeze guard raised on `worktree_moved` … and, because it raises in `__exit__`, destroyed the runtime summary. `code_stable=True`, so the cells are not void — only the tree hash moved; I reconstructed the roll-up." | same | Low | _(none — agent instrumentation, not a served surface)_ | harness-teardown-masks-result | **Non-blocking** (outside every blocking class: agent-local tooling, nothing served to callers) | n/a | `fixed` by the agent in its own harness; no repo artifact involved. |
| SELF-156-r1-01 | Emitter comment asserted "Reversing the two silently corrupts the binding" for the escape-then-substitute order. | self-review during implementation | n/a (self) | emitted XML validity | unverified-claim-in-served-comment | **Standard** (comment adjacent to emitted-XML logic; corrected before any gate ran) | uncommitted | `fixed` — the claim is FALSE: measured over 200,000 generated templates, both orders agree, because the token carries no apostrophe and neither it nor `{1}` can flip `_looks_like_json`. Comment rewritten to the true reason (oracle parity) and the equivalence pinned by `test_escape_then_substitute_and_the_reverse_agree_today`. |
| SELF-156-r1-02 | First leak-control test for the recovery mixing exemption was vacuous — a Branch leg with a non-empty prefix is refused by the PREFIX rule before ancestry is consulted. | self-review during implementation | n/a (self) | capability reachability | vacuous-guard | **Standard** | uncommitted | `fixed` — rewritten with an empty-step leg so connector ancestry is the only rule that can fire; the test now fails if the exemption leaks. |
| SELF-156-r1-03 | First cut of the containment relaxation was exploitable: forging `success_mode="continue"` onto a terminating handler silently widened its containment check and nothing objected. | self-review, mutation control | n/a (self) | emitted graph validity | permission-without-obligation | **Standard** | uncommitted | `fixed` — `success_mode` must now be EARNED: a handler declaring a continuation must reach a following region or `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` fires. Mutation control now detects it. |
| SELF-156-r1-04 | The compiler placement mirror refused the serialized-region chain the parser accepted (`PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED`). | self-review, two-entry-point differential | n/a (self) | capability reachability | two-entry-point divergence | **Standard** | uncommitted | `fixed` — `_check_try_catch_placement` now renders the same chain grammar. Found because the byte-match alone did NOT catch it: `emit_process` does not run `validate_body_capabilities`. |
| CDX-156-r1-01 | "The newly admitted `handler(continue) -> map_ref -> handler(stop)` shape always fails public compilation. `validate_connector_call_semantics()` rejects any non-`connector_call` while `pending_map` is set, so reaching the second handler raises `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`, even with matching profiles." | Stage-2 repo Codex review, round 1, run dir `cdx-review.BvI4Hx`, base `b07babeb3752d337ffa307c4c486e181e703a2c2`, head `9ab67897a308c2729b1d950130b738692216146e` | P1 | capability reachability | emitter-proved-but-pipeline-unreachable | **Standard** (source label P1, not P0/Critical; no secrets/data-loss/mutation-accounting anchor) | `9ab6789` | `fixed` — CONFIRMED by reproduction: the same chain without the map compiles, with it does not. The shape is `golden-000005`. Root cause on my side: the byte-parity tests call lowering + `emit_process` directly and never run connector resolution, so a golden the emitter reproduces perfectly was unbuildable through `compile_process_ir_v1`. Pending map pairings now cross a handler (`_MAP_CONSUMER_TRANSPARENT`) and are cleared on the catch fork; a mismatched target is still refused at `/body/steps/N/map_ref`, pinned with a negative control. |
| CDX-156-r1-02 | "Connecting protected paths here leaves `derive_error_regions()` using its unchanged, unbounded `_collect_subtree()` traversal, which absorbs later handlers and their catch bodies into the preceding protected region." | same round | P2 | mutation accounting | unbounded-region-derivation | **Standard** | `9ab6789` | `fixed` — CONFIRMED and broader than reported: region 0 absorbed BOTH later handlers including their catch bodies, and region 1 (retry 1) absorbed handler 2's catch `cache_put`. `validate_error_handling` grades retry safety over exactly that set, so a retried region answered for writes it never retries. `_collect_subtree` now takes a `stop_at` boundary derived from the graph's own handler nodes; the reviewer's `[0,1,0]` repro compiles, and an intervening map still belongs to the PRECEDING region — the reading live QA measured. |
| SELF-156-r2-01 | "Region bounding at the next handler drops the later handlers' PROTECTED paths, so a retried chain region never grades the writes it re-runs." | adversarial audit (6 lenses, 3 refuters/finding), lens `region-accounting`; workflow `wf_56b9585f-756` | critical | mutation accounting | region-bounded-on-the-wrong-axis | **Critical** (CLASS anchor: mutation accounting) | `08d6d7d` | `fixed` — CONFIRMED by reproduction: a non-idempotent DB Send is REFUSED inside a retry=2 region and ACCEPTED one handler downstream of it, though `continue` means the retried path flows into that handler. Introduced by my own fix for `CDX-156-r1-02`: bounding at the handler removed the later CATCH bodies (correct) and the later PROTECTED bodies with them (wrong), trading a false refusal for a silent acceptance. The boundary is redrawn on the SUCCESS/FAILURE split — the protected walk follows every edge except a `catch` edge — and both directions are pinned, including a control proving a recovery-only write is still not graded. |
| SELF-156-r2-02 | "A JSON-shaped notify template emits the caught-error placeholder inside MessageFormat quotes, so the logged line never carries the error the model requires it to carry"; and, from the critic, the same for any authored brace run. | adversarial audit, lenses `region-accounting` / `emission-bytes` / completeness critic | minor + standard | emitted XML or graph validity | binding-defeated-by-escaping | **Standard** | `08d6d7d` | `fixed` — MEASURED on the shipped escaper: `{"e":"<token>"}` renders `'{"e":"{1}"}'` (whole body quote-wrapped, so `{1}` is literal and no error is carried), and `order {id} failed` passes an invalid MessageFormat argument through unescaped. Both let a caller satisfy `NotifyNodeV1`'s token rule and still log a failure without the failure. A brace is now refused at authoring, with a control proving the shipped golden's own template still binds `{1}` outside any quoting. Pre-existing in the legacy #89 path; refused only on the canonical surface this slice introduces. |
| SELF-156-r2-03 | Four served texts still stated the pre-#156 grammar: the `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` remediation (#141's "move the steps into every leg or arm", meaningless for a misplaced `continue`), the `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` remediation (two placements, omitting the chain), the `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED` remediation (omitting the `process_call` recovery terminal), and both Try/Catch body schema descriptions (stale terminal sets contradicting their own `oneOf`). | adversarial audit, lenses `entrypoint-parity` / `served-contract` | standard ×4, minor ×2 | machine-served schemas/contracts | stale-served-text | **Standard** | `08d6d7d` | `fixed` — all verified against the enforcing code before editing, all corrected, schema and contract fixtures regenerated. `NotifyNodeV1`'s own description also cited a Python module path and never served the token literal; it now serves `meta.base.catcherrorsmessage` directly. |
| SELF-156-r2-04 | "The compiler can now raise PROCESS_IR_SCHEMA_INVALID_CARDINALITY, which is absent from the compiler's served message/remediation registries." | adversarial audit, completeness critic | standard | machine-served schemas/contracts | served-code-without-served-text | **Standard** | `08d6d7d` | `fixed` — CONFIRMED: `_as_compile_error` did a blanket lookup over `_CUSTOM_ERROR_CODES` (11 codes), three of which the compiler has no text for. Narrowed to a CLOSED set (`_CHAIN_RULE_CODES`) that fails closed on anything else, the one reachable code is now served by the compiler's own tables, and `test_the_chain_rule_codes_are_served_by_the_compiler` fails if the shared rules widen without served text. Registering it then tripped the supply-side guard (no served row for a code nothing can raise) because the code is raised dynamically; recorded per-code in `COMPILER_REGISTERED_PARSE_CODES` with its reason. |
| CDX-156-r2-01 | "Both new rows incorrectly derive Standard: `CDX-156-r1-01` carries a P1 source label, and `CDX-156-r1-02` is classified as mutation accounting. Either anchor requires Critical … Marking these findings fixed does not resolve the inconsistent audit history, which the completion policy explicitly treats as invalidating closure." | Stage-2 repo Codex review, round 2, run dir `cdx-review.3LGJmM`, base `9ab67897a308c2729b1d950130b738692216146e`, head `3fd32fd69f9ca47ca9899ca5860a1eb825aa7d58` | P2 | _(audit record, not a served surface)_ | tier-derived-against-its-own-anchor | **Standard** | `3fd32fd` | `fixed` — CONFIRMED against the rule verbatim. My recorded rationale enumerated the anchor list and then excluded a member of it. Both rows re-derived Critical with the anchor named; `QA-156-r0-01` carried the same defect (High is also a listed anchor), was not flagged, and is corrected too. No source label altered. |
| QA-156-r0-01a | Revision of `QA-156-r0-01` (original retained, byte-frozen), raised by self-audit while correcting CDX-156-r2-01 | Same finding; the derived TIER was wrong. The source labelled it High, and the tier table names P0/P1/Critical/High, so the anchor is a label anchor and the tier is Critical. My original rationale enumerated that list and then excluded High from it. | High (unchanged) | QA fixture provenance | dispatch-time-race | **Critical** (LABEL anchor: High) | `3fd32fd` | `fixed` — disposition unchanged and still valid: the oracle's independence was re-proved from a pristine `git archive b07babe` extraction with all five artifacts byte-identical. Re-tiering adds no residue; a Critical row may not be deferred and this one is not. |
| CDX-156-r1-01a | Revision of `CDX-156-r1-01` (original retained, byte-frozen), raised by Stage-2 repo Codex review, round 2, run dir `cdx-review.3LGJmM` | Same finding; the derived TIER was wrong. A P1 source label is a critical anchor. | P1 (unchanged) | capability reachability | emitter-proved-but-pipeline-unreachable | **Critical** (LABEL anchor: P1) | `3fd32fd` | `fixed` — disposition unchanged: the pending map pairing now crosses a handler and is cleared on the catch fork, with a negative control pinning that a mismatched target profile is still refused at the map's own pointer. Validated on the current tree. |
| CDX-156-r1-02a | Revision of `CDX-156-r1-02` (original retained, byte-frozen), raised by Stage-2 repo Codex review, round 2, same run | Same finding; the derived TIER was wrong. The row's own blocking class is mutation accounting, which is a class anchor. | P2 (unchanged) | mutation accounting | unbounded-region-derivation | **Critical** (CLASS anchor: mutation accounting) | `3fd32fd` | `fixed` — disposition unchanged: protected regions are bounded at sibling handlers, the reviewer's `[0,1,0]` repro compiles, and per-handler region membership is asserted. Validated on the current tree. |
| CDX-156-r1-03 | "the same served entry still says a connector scope must follow the producing call, while the generated `TryCatchNodeV1` schema says nothing may follow a Try/Catch." | same round | P2 | machine-served schemas/contracts | stale-served-text | **Standard** | `9ab6789` | `fixed` — CONFIRMED in both places. The projection summary and the `TryCatchNodeV1` docstring (served as the schema `description`) both stated the pre-chain grammar. Both corrected; schema and contract fixtures regenerated. |
| ARCH-156-r1-01 | "The Continue grammar is too restrictive. `[connector_call, handler(Continue), handler(Stop)]` is rejected as an orphan continuation, although both the existing prefix and the handler chain are individually legal." | §6 architect implementation review, evaluation 1, run dir `cdx-gate-review.LTjjBy`, attested `ok:true` | Standard | capability reachability | unjustified-narrowing | **Standard** | `ae03d1d` | `fixed` — CONFIRMED. `_is_serialized_region_chain` required the root to BEGIN at a handler, so a legal producing-call prefix composed with a legal chain was refused, and refused with an orphan-continuation diagnostic describing neither half. The recognizer now takes the prefix as whatever precedes the first handler and validates the handler suffix separately; the prefix vocabulary is checked explicitly (`connector_call` + root-linear) because this branch returns before the connector_call-sequence rules run. |
| ARCH-156-r1-02 | "Continuation containment does not identify the authorized successor. It accepts any root Map or handler." | same | Standard | emitted XML or graph validity | imprecise-escape-target | **Standard** | `ae03d1d` | `fixed` — CONFIRMED by inspection of the check itself: it tested only that the escape was a root-level map or handler. My two attempts to build the reviewer's consistently-rewired 0→2→1 mutant were each caught incidentally by the reachability invariant, which is NOT the same as this check being precise. The escape must now land on the declaring handler's authored index + 1, derived from its own `source_path`. |
| ARCH-156-r1-03 | "The planned independent emission safeguards are missing. Change either lowered recovery flag to false and generate a matching plan." | same | Standard | mutation accounting | plan-checked-only-against-its-own-cfg | **Critical** (CLASS anchor: mutation accounting) | `ae03d1d` | `fixed` — CONFIRMED by reproduction: a CFG carrying `abort_on_error=False` with a plan lowered from it passed BOTH checkers and emitted `abort="false"` — the silent flip the issue forbids. Every other plan check asks "does the plan agree with the CFG", which is the one question a lowering defect answers yes to. `check_emission_plan_invariants` now reads the EMITTED input against the contract itself (both flags true, no outgoing transition) with no CFG counterpart to agree with. Both flags pinned by mutation, with the unmutated control passing. |
| ARCH-156-r1-04 | "Notify validation still admits a template that defeats binding. `["meta.base.catcherrorsmessage"]` passes full compilation and emits `'["{1}"]'`. Separately, a constructed Notify input with `message_template=None` raises raw `AttributeError`." | same | Standard | emitted XML or graph validity | rule-cut-on-the-wrong-predicate | **Standard** | `ae03d1d` | `fixed` — CONFIRMED both halves. My brace ban was cut on the wrong predicate: the hazard is the escaper's JSON quote-wrapping, which a JSON ARRAY triggers with no brace at all. The rule is recut on "would this template defeat the binding" (leading `[` after stripping, or any brace), and the emitter preflight type-checks before calling `.strip()`. |
| ARCH-156-r1-05 | "Canonical diagnostic mappings are incomplete." Blank/missing-token Notify served a cardinality code; a catch-step ProcessCall served body-placement rather than the connected-call identity; root Notify serves `PROCESS_IR_CAPABILITY_UNSUPPORTED`. | same | Standard | machine-served schemas/contracts | wrong-diagnostic-identity | **Standard** | `ae03d1d` | `fixed` (two of three) — the template rules now serve `PROCESS_IR_SCHEMA_INVALID` via a new `_schema_invalid_error` (a template content rule is not a list bound or an ordering rule), and `_translate_pydantic_error` gained `catch_body` so a call in catch STEPS serves the connected-call identity like every other call-capable body. **PARTIAL REFUTATION on root Notify**: that refusal comes from the pre-existing generic root rule shared by every kind the root sequence does not admit, `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` names a body the root is not, and re-coding it for `notify` alone would make its root refusal differ from every other inadmissible kind's. Recorded as a refutation with evidence rather than changed. |
| ARCH-156-r1-06 | "Required golden migration and independent comparisons are unfinished. Both existing Notify corpus entries still use the legacy builder … the claimed canonical comparison against the independent B2 capture is absent." | same | Standard | emitted XML or graph validity | claim-measured-but-never-pinned | **Standard** | `ae03d1d` | `fixed` in the part that matters, with a recorded deviation on the rest. `golden-000005` had NO permanent byte-parity pin — the equality was measured by hand mid-implementation, reported, and never written down, so every later change to region derivation, connector resolution and the plan invariants ran without it. Now pinned, along with a test asserting the new `golden-000075` matches the archived live B2 capture with ids/families/labels blinded. **DEVIATION (recorded, not silently skipped)**: the two corpus cases are NOT re-pointed to canonical rendering. Doing so would stop the legacy builder being exercised for those shapes, making the goldens photographs of the code under test; the current arrangement renders them through the legacy ORACLE and asserts canonical equality in a test, which is a genuine differential. The acceptance criterion ("both legacy notify goldens are reproduced byte-identically from canonical IR") is met by those tests. |
| ARCH-156-r1-07 | "Served ordering guidance contradicts Map admission. The projection says nothing may follow a handler except another handler, omitting the permitted intervening Map." | same | Standard | machine-served schemas/contracts | stale-served-text | **Standard** | `ae03d1d` | `fixed` — the ordering fact now names the optional single `map_ref` between handlers. |
| SELF-156-r3-01 | A pre-existing control test authored a `process_call` in a catch body to prove body-slot remapping; #156 made that kind admissible in the catch TERMINAL, so the witness silently became a test of the other rule. | self-review while fixing `ARCH-156-r1-05` | n/a (self) | _(test witness, not a served surface)_ | witness-invalidated-by-the-change | **Standard** | `ae03d1d` | `fixed` — witness replaced with a `branch`, which remains inadmissible in a catch body in every slot, so the control tests the property it names. |
| ARCH-156-r2-01 | "Continue grammar — original defect fixed; new guard bypass. The prefix check validates vocabulary but skips the existing cache-consumption rule." | §6 architect implementation review, evaluation 2, run dir `cdx-gate-review.2RTrFA`, attested `ok:true`, base `ae03d1d`, head `48d7ea0` | Standard | runtime behavior | widened-grammar-skips-its-own-guards | **Standard** | `48d7ea0` | `fixed` — CONFIRMED and it is a REGRESSION I introduced in evaluation 1's fix: `[call, cache_put, call, handler(stop)]` was refused and the identical prefix followed by a two-handler chain was ACCEPTED, because the chain branch returns before `_sequence_rules` reaches the Add-to-Cache consume rule. The rule is now re-run inside the chain branch. Widening a grammar must not narrow what the grammar it widened still enforces. |
| ARCH-156-r2-02 | "Containment — partially fixed. A Map handoff still stops traversal immediately, leaving the Map's successor unchecked — contrary to the new comment. A four-handler mapped chain consistently rewired to execute authored handlers 0 → 4 → 2 → 6 passes both CFG and plan invariants." | same | Standard | emitted XML or graph validity | imprecise-escape-target | **Standard** | `48d7ea0` | `fixed` — a HANDLER escape ends the walk (the next region owns what follows); a separator MAP does not, so the walk now continues through it with the successor requirement advanced, and a map handing off to the wrong handler is refused. My evaluation-1 comment claimed this was already handled; it was not. |
| ARCH-156-r2-03 | "Independent emission safeguards — flags fixed; ordering still required. Both checkers still accept consistently mutated CFG/plan pairs that allocate catch1 before the second handler, or swap the complete catch blocks." | same | Standard | mutation accounting | plan-checked-only-against-its-own-cfg | **Critical** (CLASS anchor: mutation accounting) | `48d7ea0` | `fixed` — I had shipped the flag half and recorded the ordering half as an open gap rather than closing it. The expected layout is now derived from AUTHORED provenance (main spine first, recovery blocks in authored handler order, each contiguous), which is a fact about the document rather than about the lowering, so a lowering that reordered blocks cannot also move the yardstick. Swapped catch blocks refused; control passes. |
| ARCH-156-r2-04 | "Notify templates — partially fixed, with a new rejection regression. A mutated Notify emission input containing a JSON array still passes preflight … Rejecting every leading `[` also rejects safe, previously accepted text such as `[ERROR] caught <token>`." | same | Standard | emitted XML or graph validity | rule-cut-on-the-wrong-predicate | **Standard** | `48d7ea0` | `fixed`, both halves, and the second is a REGRESSION I introduced: my predicate was cruder than the escaper's own test and refused ordinary bracket-prefixed text the escaper leaves alone. The predicate now mirrors `_looks_like_json` exactly (leading `{`/`[` AND parses as an object/array) and is shared with the emitter preflight, so the binding rule holds at BOTH boundaries. The legal-template fixture list was corrected too — it carried a JSON body that is no longer authorable. |
| ARCH-156-r2-05 | "S22 explicitly assigns `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` to Notify outside admitted catches. The generic root implementation does not refute that contract, and this newly introduced canonical node has no pre-slice root diagnostic to preserve." | same | Standard | machine-served schemas/contracts | wrong-diagnostic-identity | **Standard** | `48d7ea0` | `fixed` — my evaluation-1 refutation is WITHDRAWN. The rebuttal is correct: `notify` is new in this slice, so there is no pre-slice root diagnostic to preserve, and handling it explicitly changes no other kind's refusal. Root `notify` now serves the contracted identity with a step-precise pointer; the control confirms a misplaced `source` keeps its generic refusal. |
| ARCH-156-r2-06 | "Goldens — useful comparisons added; deviation insufficient. The planned canonical corpus route exercises normalization, compilation, late binding, materialization, and complete envelope bytes." | same | Standard | emitted XML or graph validity | coverage-not-taken | **Standard** | `48d7ea0` | `finding-refuted` on NEW evidence, my original argument WITHDRAWN. The architect correctly demolished my first reason (that migrating would make the goldens photographs of the code under test — the frozen bytes stay frozen either way, so the canonical route genuinely adds envelope coverage). MEASURED blocker instead: both goldens drive a REST **POST** target, and the connector-call capability registry lists only `get` and `patch` for the REST family, so the graph is not authorable as a canonical `connector_call` chain and `compile_process_ir_v1` refuses it at `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED` before any envelope is built. Connector capability rows are explicitly OUT of scope for #156 (the issue assigns them to #155). Pinned by `test_the_notify_goldens_cannot_take_the_canonical_corpus_route_yet`, which FAILS the day a REST write intent is registered, so the migration is picked up rather than forgotten. |
| SELF-156-r4-01 | The slice's own canonical byte-parity documents authored `action: "read"` / `"write"`, which are not members of the connector-call capability vocabulary; they passed only because the shapes path (lowering + emit) never runs the capability check. | self-review while attempting `ARCH-156-r2-06` | n/a (self) | _(test fixture, not a served surface)_ | fixture-authored-outside-the-vocabulary | **Standard** | `48d7ea0` | `fixed` — corrected to the registered intents (`get`, `send`). Found only because the corpus-migration attempt drove the same documents through the FULL pipeline, which is the check the shapes path skips. |
| ARCH-156-r2-06a | Revision of `ARCH-156-r2-06` (original retained, byte-frozen), raised by §6 architect evaluation 3, run dir `cdx-gate-review.y3Jowm` | Same finding; the DISPOSITION was wrong. Evaluation 3 accepted the deferral BASIS — "#156 should not add REST POST support merely to unblock these goldens" — but ruled that `finding-refuted` is the wrong disposition for missing coverage, and that generic ownership by the already-closed #155 does not supply one. It also corrected the witness: it asserted `post` against a PATCH operation, exercising the assertion-mismatch refusal rather than the capability gap. | Standard (unchanged) | emitted XML or graph validity | coverage-not-taken | **Standard** | `f7b3687` | `deferred` — reason class **`blocked-by-mechanism`** (the canonical connector-call registry lists no REST write intent, so the graph is not authorable through the public route at all). Witness corrected to a real POST operation with the `action` assertion OMITTED, asserting the code AND the `/operation_ref` pointer that distinguishes the capability gap from an assertion mismatch. PLACEMENT: the follow-up issue enumerating the two corpus migrations and their full-envelope acceptance criteria is prepared but NOT filed — filing is the owner's call in this repo, so it is surfaced as a manual step in the closing report and this row cannot close until it is filed and sequenced. |
| ARCH-156-r3-01 | "Root Notify identity — Partially closed; Standard blocking contract defect remains. The chain branch bypasses the new check. `[call, notify, handler(continue), handler(stop)]` returns `PROCESS_IR_CAPABILITY_UNSUPPORTED` at `/body`. The equivalent ordinary root correctly returns `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` at `/body/steps/1`. Apply the placement check before that branch and pin both cases." | §6 architect implementation review, evaluation 3, run dir `cdx-gate-review.y3Jowm`, attested `ok:true`, base `48d7ea0`, head `f7b3687` | Standard | machine-served schemas/contracts | wrong-diagnostic-identity | **Standard** | `3deba2b` | `fixed` — CONFIRMED by reproduction, and it is the incomplete closure of `ARCH-156-r2-05` rather than a new claim: my evaluation-2 fix put the rule below a branch that returns early. The check is hoisted above every branch and both cases are pinned. This is the SECOND instance of one mechanism (a root rule placed after an early-returning branch) and it is discharged structurally by `SELF-156-r5-01`, not by this hoist alone. |
| ARCH-156-r3-02 | "Recovery/chain regressions: blocking validation work for this slice. The finite remaining cases are: (1) a recovery child's state read cannot use its own failed try's writes … (2) a valid continuing-chain write succeeds with its grant; transplanting that grant across call path/sibling, operation, or root fails; (3) a legal recovery/chain request reaches public planning and materialization with child references/dependency order intact; revoking required evidence after planning prevents mutation; (4) reuse that request across two account bindings to verify relocatable identity and correctly rebound child references." | same | Standard | runtime behavior | coverage-not-taken | **Standard** | `3deba2b` | `fixed` — all four cases added as regressions over the newly admitted graph forms, driven through the public planning and materialization path rather than the compiler alone. The architect's own scoping is honoured: the cases it said were already covered (retry membership, later protected-write refusal, exclusion of recovery-only writes) got no second round, and the relocation test it flagged as using the older document form was rewritten. |
| ARCH-156-r3-03 | "Architecture narrative: advisory; follow-up acceptable. The old Try/Catch narrative remains inaccurate, but it is static documentation. Apply the permitted prose batch or record the remaining work; it does not independently block this slice." | same | Standard | _(none — architecture prose, not served to callers)_ | stale-authority-prose | **Non-blocking** (outside every blocking class: internal architecture narrative, nothing machine-served) | `3deba2b` | `fixed` in the single permitted non-blocking batch, folded into the same correction rather than earning a batch of its own. The narrative now describes the admitted catch-leg terminal and the serialized-region chain. |
| ARCH-156-r3-04 | "Notify witness re-homing: nonblocking ownership debt; follow-up acceptable. Canonical registration/emission and frozen Notify bytes already have independent coverage. Moving the remaining XML/doctrine witnesses matters for legacy removal, but their present ownership alone is not a behavior defect." | same | Standard | _(none — test ownership, not a served surface)_ | witness-ownership-debt | **Non-blocking** (the architect's own words: not a behavior defect) | `f7b3687` | `finding-refuted` on the OBLIGATION, not on the observation. Disputed claim: that this slice owes the move. Evidence: the canonical surface already carries its own independent witnesses — the caught-error token and level-vocabulary authority pins, the registry registration, the emitter preflight matrix and the two frozen-golden byte comparisons, none of which route through the legacy builder. The witnesses that stay behind are witnesses OF the legacy emitter and are correctly owned by it until it is removed; the issue's own scope note says to coordinate with the re-homing slice and not duplicate it, so moving them here would create the duplicate that note forbids. |
| SELF-156-r5-01 | The mechanism behind `ARCH-156-r3-01` was still live for a second kind: with `notify` hoisted, a root `continue` inside a chain prefix still served the chain's generic vocabulary refusal (`PROCESS_IR_CAPABILITY_UNSUPPORTED`) while the identical prefix outside a chain served its contracted continuation identity. | self-review after evaluation 3, measured by a differential over the whole root vocabulary (343 prefix documents, then the same documents with the chain branch disabled) | n/a (self) | machine-served schemas/contracts | wrong-diagnostic-identity | **Standard** | current delta | `fixed` STRUCTURALLY, as the second instance of this pair requires. The per-branch placement is replaced by one table of the kinds no legal root may contain, consulted by the ordinary grammar AND by the chain grammar itself — so the compiler mirror, which reaches that grammar through the same helper, inherits it instead of needing the rule a third time. SIBLING SWEEP: all 22 members of the root step union were enumerated from the union itself; two carry a refusal and the other twenty are legal in some root shape, with the near-miss recorded — a process call is NOT a member, because the exact singleton is a legal root and its own root authority deliberately yields when a control node is present, so the vocabulary refusal is the answer that authority already prescribes. NON-VACUITY: two mutants were hand-run — the chain grammar not consulting the table, and the table consulted only after the chain branch (the exact pre-fix arrangement) — failing 2 and 4 of the new witnesses respectively, so the witnesses would have caught both this defect and the architect's original one. COVERAGE CLAIM: the membership test derives its candidates from the root union at run time, so a kind added to the union without a decision fails it. |
| SELF-156-r5-02 | The compiler's translation of a shared chain rule was gated on a hand-listed set of "the codes the shared chain rules can raise". It did not cover the chain grammar's own prefix-vocabulary refusal — which predated the list — so a mutated model at the public compiler entry point surfaced a raw validator exception with no code and no pointer; the never-admitted-kind table would have added a second uncovered code. | self-review while auditing `SELF-156-r5-01`, measured by driving both refusals through the public compiler entry point | n/a (self) | machine-served schemas/contracts | aggregate-a-set-fails-open | **Standard** | current delta | `fixed` STRUCTURALLY, same class as `SELF-156-r5-01` one layer down and the same remedy: the hand-list is replaced by a predicate that reads the SERVED TABLES — translate when this layer can render the code, re-raise loudly when it cannot. The old test asserted the wrong direction (every LISTED code has text, never that the list covered what the rules raise), which is why it stayed green through both gaps; the replacement asserts BOTH directions over the model's whole code map and pins a control code that must NOT translate. NON-VACUITY: the old closed set was restored as a mutant and failed 3 of the new witnesses. |
| SELF-156-r5-03 | The same call site rendered every translated refusal at the pointer `/body`, discarding the step position a shared rule had attached to it — so the two entry points would agree on the code and disagree on where to look. | self-review, same audit | n/a (self) | machine-served schemas/contracts | two-entry-point divergence | **Standard** | current delta | `fixed` — the position already travels in the refusal's own context, untouched by pydantic, so the compiler now renders it instead of a constant. NON-VACUITY: the hard-coded pointer was restored as a mutant and failed the never-admitted-kind witness, which asserts the served path and not only the code. |
| QA-156-r2-01 | "The hoist changes precedence on doubly-invalid documents. Over an 8-row probe of roots violating two rules at once, one row moves: a root with a misplaced source AND a root continue served the cardinality code pre-fix and now serves the continuation code. The other 7 rows are identical." | Stage-1 QA, round 2 (live, through the public MCP tool boundary), recorded by the agent as a measured NON-finding | n/a (recorded observation) | machine-served schemas/contracts | precedence-shift-on-multi-defect-input | **Non-blocking** | current delta | `finding-refuted` on the only claim that would matter, with the agent's own measurement as the evidence: both trees REFUSE the document, no verdict flipped anywhere in its 48-cell blast-radius sweep, and the one row that moved now serves the more specific of two co-present rules — the identity the shared table contracts — rather than a cardinality code describing the other defect. Which of several simultaneous violations is reported first has never been contracted, and reporting the more specific one is the improvement, not the regression. Recorded here so it is not re-discovered as a defect. |
| SELF-156-r5-04 | Retiring a test the slice itself had added, and renumbering the rows behind it, is refused twice over: the wave gate rejects a row appended already tombstoned, and the successor guard rejects the deletion-plus-repoint that removing it requires, because the committed manifest is already legal at the landing base and so the regeneration repairs nothing. | self-review, measured by running the manifest gate and the successor guard against the working tree | n/a (self) | _(none — test manifest, not a served surface)_ | same-range-retirement-is-not-a-repair | **Non-blocking** | current delta | `fixed` by not creating the situation: the rewritten witness KEPT its original node id instead of being renamed, so the manifest change is append-only — seven new rows, no tombstone, no repoint, floor 11661. Recorded because the kept name is now slightly narrower than what the test asserts, and that is a deliberate trade rather than an oversight: the node id is the manifest's identity and this repository will not repoint one to improve a name. |
| QA-156-r3-01 | "`build_integration(action=\"plan\")` never reaches the compiler at all — instrumented, the private core is called zero times on all ten plan rows. Both A instruments drive `action=\"plan\"`, so their zero is expected and cannot by itself speak for a compiler-layer delta." | Stage-1 QA, round 3, recorded by the agent against its OWN evidence | n/a (agent self-correction) | _(none — scope of a probe, not a served surface)_ | probe-cannot-reach-the-layer-it-grades | **Non-blocking** | current delta | `fixed` in the record rather than the code: the finding is CORRECT and it invalidates the inference I would otherwise have drawn from part A. The compiler-layer claim rests entirely on part B, which reaches that layer directly. Recorded because a reader meeting two green instruments could reasonably read them as covering a layer they never execute — the same shape as this slice's earlier vacuous-guard defects, caught here by the agent grading its own probe instead of its result. |
| QA-156-r3-02 | "The legacy builder's emission adapter I closed by call-graph reasoning only. It calls the public compile entry point with a caller-owned model, so it inherits the same unconditional re-parse — but I did not drive it live; I could not build a valid legacy spec to do so." | Stage-1 QA, round 3, recorded by the agent as an inconclusive cell | n/a (self-reported limit) | capability reachability | route-closed-by-argument-not-by-execution | **Non-blocking** | current delta | `not-validated`, stated as such rather than counted as a negative. What IS established is stronger than the missing cell: the mirror has exactly ONE caller, that caller is private, absent from the module's exported names, and reached only through three entry points that all re-parse first — so the property holds over ALL routes rather than over the routes anyone thought to drive, and this adapter calls the public entry point like any other caller. The agent's own rule for the gap is recorded with it: a route that fails its own parameter validation is an inconclusive cell, never a negative. |
| ARCH-156-r4-01 | "Calling the acceptance criterion fully satisfied without qualification is too favorable: the approved plan explicitly requires matching envelope metadata and COMPLETE emitted fixture bytes, as well as migration through the canonical corpus route." | owner-requested architect consultation, attested `ok:true` (memo at `.codex/plans/issue-156-consultation.md`), then independently verified by five falsification-first checks | Standard | emitted XML or graph validity | claim-measured-but-never-pinned | **Standard** | current delta | `fixed` — CONFIRMED, and the shortfall was MEASURED rather than argued: the shapes section is 3180 of golden-000059's 3759 bytes and 5180 of golden-000005's 5759, so each comparison left 579 bytes unchecked — the component wrapper and all seven process-level attributes, exactly the envelope metadata the plan names. Both goldens now have a COMPLETE-FILE byte pin through emit + materialize, with four non-vacuity controls (the emitter's own output must NOT equal the golden; dropping a shape part must break it; a wrong name, profile or folder must break it; the profile must be DERIVED, not chosen). The plan's other golden bullet — configuration bytes against the legacy builder — was already discharged, because every configuration element lives inside the shapes section; the correction is scoped to what was actually missing. |
| ARCH-156-r4-02 | "The witness docstring tells a future reader that when a REST write intent is registered the migration becomes possible and should be done." | owner-requested architect consultation, attested `ok:true` (memo at `.codex/plans/issue-156-consultation.md`), then independently verified by five falsification-first checks | Standard | machine-served schemas/contracts | overclaimed-served-text | **Standard** | current delta | `fixed` — CONFIRMED and MEASURED per golden against a synthetic capability row, which is what showed the claim is half true and therefore worse than plainly wrong: for the single-handler golden the row IS necessary and sufficient — it then compiles and emits shapes byte-equal to the frozen file — while the double guard is refused again on missing idempotency evidence, and once that is authored refused a third time on a profile mismatch at the map. The docstring now states both cases separately with their measured codes, records why the idempotency code is the one that appears (the default classification is upgraded from the packaged replay registry's observed verdict, so a reader expecting the fail-closed default will hunt the wrong diagnostic), corrects a loose reference to "two retries" when exactly one region is retried, and labels its own failure a trigger to RE-DECIDE rather than a certificate. This text is designated by this ledger as where a reader will be standing when it matters, so it is blocking, not prose residue. |
| SELF-156-r6-01 | The closing report stated it "carries the prepared issue body verbatim"; it did not. The body existed only in a scratch file outside the repository, and the deferral section and the closing report pointed at each other in a circle around a document that was never written. | self-review prompted by the consultation, then confirmed by an independent read of the whole ledger | n/a (self) | _(none — audit record, not a served surface)_ | claim-measured-but-never-pinned | **Standard** (the audit record is not served to callers, but a false statement in it invalidates the closure it supports, so it is corrected to the same bar) | current delta | `fixed` STRUCTURALLY, as the THIRD instance of this pair requires. The body is now written out in full in the closing report rather than the sentence being deleted. SIBLING SWEEP over every cross-reference in this ledger asserting a durable artifact: all run directories, report paths and harness paths resolve. INVARIANT, because two instances had already been instance-patched inside this slice: a repo-wide test now asserts that every backticked in-repo path any audit ledger cites resolves against the GIT INDEX by path suffix — the rule the ledgers actually follow — with gitignored roots excluded first and a frozen baseline of four pre-existing dangling citations in three other slices' ledgers that cannot grow. NON-VACUITY: two mutants hand-run — a fabricated citation added to this ledger fails the invariant, and a baseline padded with a path that DOES resolve fails the freshness check. |
| SELF-156-r6-02 | The consultation recommended citing issue #159 as the already-filed, sequenced home for the deferred row, on the grounds that it already requires resolving capability prerequisites. | verification of the consultation's own recommendation before acting on it | n/a (self) | _(none — a proposed disposition, not a defect)_ | placement-asserted-not-verified | **Non-blocking** | current delta | `finding-refuted` — the load-bearing word is "already", and it is false. Verified against the issue body: #159 is open, in the same milestone, owns caller migration, carries survivor-golden byte-identity criteria and is sequenced before #160 — but its body never names either of these two goldens, never mentions the corpus route, and its own out-of-scope clause excludes the very act that unblocks the work; its single notify reference is a different golden pair whose action is retirement, not migration. Citing it unedited would be paper compliance, not placement. Recorded because the recommendation came from a gate and was rejected on evidence rather than adopted — and because the alternative it displaced (minting a new issue) is the one this repository disfavours, so the choice between them is the owner's, not mine. |
| QA-156-r4-01 | "The new ledger invariant's frozen baseline is untracked, so two of its three tests die on a clean checkout. `tests/fixtures/audit_ledger_path_citation_legacy_baseline.json` is not in the git index; the three new tests pass in your working tree only because the file exists there. And the shape is the same class the batch exists to close — defect 3 was a document that existed only outside the repository; here the new invariant's own frozen baseline exists only outside the git index." | Stage-1 QA, round 4 (darkness proof + adversarial grading of the new guards) | **High** | capability reachability | artifact-exists-only-outside-the-index | **Critical** (LABEL anchor: High) | current delta | `fixed` — CONFIRMED by the agent's own CI simulation, which is the part that made it undeniable: a worktree containing every tracked file plus the uncommitted edits and no untracked files fails both tests with `FileNotFoundError`. Not merely red, either — the node manifest already pins the three tests and raises the floor, so the failures are mandatory rather than skippable. The file is tracked now, and the same CI simulation re-run on the fixed tree passes 14 of 14. The irony is recorded rather than smoothed over: the invariant written to stop a document existing only outside the repository was itself relying on one. |
| QA-156-r4-02 | "The citation invariant's extension allowlist misses the only genuine false citation in the tree — in #156's own ledger. It cites an architect-reviews run directory for the consultation (name elided — see below) which does not exist anywhere in the repo; the invariant does not see it because that suffix is not one of the eight listed extensions. Sized across the fourteen ledgers: 704 run-directory citations, of which exactly one resolves to nothing." | same | Medium | machine-served schemas/contracts | guard-blind-to-the-shape-it-most-needs-to-see | **Standard** | current delta | `fixed` on BOTH halves, because either alone would have been an instance patch. The false citation was mine and it was real: I cited an archive directory for the consultation that was never created, and could not be — the archive contract covers commit reviews, implementation-review turns and wave-gate runs, and it correctly REFUSED a plan-kind round rather than letting me file one under the wrong kind. The memo now cites where it actually lives. The guard was then widened to match archive run directories by SHAPE rather than by extension, and — the part the shape arm exposed — its resolver was taught to resolve DIRECTORIES: git tracks no directory of its own, so 335 correct run-directory citations across the ledgers had been resolving against nothing. NON-VACUITY: the exact citation the agent found was re-introduced as a mutant and now fails the guard. The offending path is NOT written out in this row, and that is the guard's contract rather than squeamishness: a backticked in-repo path is a CLAIM that the artifact exists, so quoting a defective citation in its own finding row would re-assert the very claim the row reports — the guard would fail on the record of its own success. Quote such a path unbackticked or describe it. |
| SELF-156-r6-03 | The agent's own grading found one real mutant the new pins do not catch: a materializer that stopped honouring `description` changes no byte the pins compare, because they only ever pass its canonical empty value. The same holds for `extension_connections`. | recorded from the QA agent's adversarial grading, which classified every mutant BEFORE reading its result and discarded three no-ops wearing a mutant's clothes | n/a (self) | _(none — a stated boundary, not a defect)_ | pins-prove-less-than-they-appear-to | **Non-blocking** | current delta | `not-validated` as a defect, and recorded as the EXACT boundary of what the pins prove, which is the honest disposition. Three of the five envelope inputs are perturbed by the controls; the other two are only ever passed their canonical values, so a materializer that stopped honouring either would go unseen. The docstrings claim only what the controls establish — the name and the execution profile — so nothing served is false. Recorded because the agent volunteered it against its own result, and because the next person to widen these pins should start here. |

**Supersession map** — `QA-156-r0-01a → QA-156-r0-01`, `CDX-156-r1-01a → CDX-156-r1-01`,
`CDX-156-r1-02a → CDX-156-r1-02`, `ARCH-156-r2-06a → ARCH-156-r2-06`. Each original is retained byte-frozen; the revision row
carries the corrected DERIVED tier and nothing else. A raw source label is immutable and
none was touched — High and P1 still read as they were reported. A field this record may
correct is read from the row that STANDS; a raw source label is read from the original.

### Critical residue

Three rows derive **Critical**, and the derivation is recorded above with its anchor rather
than chosen. Each is `fixed` and validated on the current tree, so there is no critical
residue: `QA-156-r0-01` (oracle independence re-proved from a pristine `git archive`
extraction, all five artifacts byte-identical), `CDX-156-r1-01` (map pairing carried across
a handler, with a negative control pinning that a mismatched profile is still refused), and
`CDX-156-r1-02` (protected regions bounded at sibling handlers, with the reviewer's own
`[0,1,0]` repro compiling and region membership asserted per handler). None is deferred and
none is closed over.

The tiers were corrected at Stage-2 evaluation 2 (`CDX-156-r2-01`). The original source
labels are UNCHANGED — High and P1 stay on the rows — because a raw source label is
immutable; only the derived tier moved, in the direction the anchors require. The
`QA-156-r0-01` correction was NOT raised by the reviewer: it carried the same misreading
(High is a critical anchor) and is fixed here for the same reason.


## Checkpoint records

Each checkpoint records: loop identity, window and cumulative evaluation numbers, current SHA and
dirty state, per-tier counts and breadth, new/resolved/recurring defect classes, trend evidence,
outcome and rationale.

| # | Loop | Window / cumulative | SHA (dirty) | Critical | Standard | Defect classes | Outcome | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | L2 — Stage-2 repo commit review | window 3 / cumulative 3 | `5edf543` (clean) | 0 unresolved | 0 unresolved | new: none this round. Resolved this window: `emitter-proved-but-pipeline-unreachable`, `unbounded-region-derivation`, `stale-served-text`, `tier-derived-against-its-own-anchor`, `region-bounded-on-the-wrong-axis`, `binding-defeated-by-escaping`, `served-code-without-served-text`. Recurring: none | **CLOSE-CLEAN** | The third evaluation returned NO findings (`cdx-review.SWQTjh`, base `3fd32fd`, head `5edf543`, `STATUS: completed`, collector exit 0, teardown confirmed), so the checkpoint closes the loop normally rather than forcing a continue/defer decision. Trend across the window is monotone: round 1 raised 3 (1×P1, 2×P2), round 2 raised 1 (P2, record-only), round 3 raised 0; highest unrefuted severity fell P1 → P2 → none; affected-class breadth fell from three blocking classes to one to none. Every finding in the window is `fixed` and validated on the current tree — none deferred, none refuted-without-evidence, no critical residue. |
| 2 | L3 — Architect implementation review | window 3 / cumulative 3 (the cap) | `3deba2b` (clean) | 0 unresolved | 0 unresolved blocking; 1 recorded accepted limitation | new this window: `widened-grammar-skips-its-own-guards`, `witness-invalidated-by-the-change`, `fixture-authored-outside-the-vocabulary`, `stale-authority-prose`, `witness-ownership-debt`. Resolved this window: `unjustified-narrowing`, `imprecise-escape-target`, `plan-checked-only-against-its-own-cfg`, `rule-cut-on-the-wrong-predicate`, `claim-measured-but-never-pinned`, `widened-grammar-skips-its-own-guards`. Recurring: `wrong-diagnostic-identity` (evaluations 1, 2 and 3 — discharged structurally, see the row below the table), `imprecise-escape-target` and `plan-checked-only-against-its-own-cfg` (each patched once, then closed on the reviewer's own repro) | **CLOSE-CLEAN** | The three-evaluation cap in `docs/architecture/COMPLETION_WORKFLOW_RULES.md` ends this loop on evaluation 3; the review itself said so ("This ends the architect loop … No fourth architect evaluation is required"). The cap's closure condition was met in full: the finite final batch was applied, its affected validation ran, and the Stage-2 commit review over the correction delta returned CLEAN. Trend across the window is improving on every recorded axis: evaluation 1 raised 7, evaluation 2 raised 6, evaluation 3 raised 4 of which 2 are outside every blocking class; highest unrefuted severity fell from Critical (twice, both mutation accounting) to Standard to Standard; four of the fourteen were regressions I introduced while fixing earlier ones and none of those recurred. Zero critical residue. The one standard blocking-class row not fixed is `ARCH-156-r2-06a`, reason class `blocked-by-mechanism`, enumerated and pinned by a self-invalidating witness but not yet attached to a filed issue — the section below states exactly what that leaves open, and it gates CLOSING the issue, not landing the work. |
| 3 | L4 — Composite wave gate | window 3 / cumulative 3 | `ca2b051` (clean) | 0 unresolved | 0 unresolved | new: none — both earlier refusals were manifest and audit-record hygiene, not defect classes in the product. Resolved this window: `same-range-retirement-is-not-a-repair`. Recurring: none | **CLOSE-CLEAN** | The composite set is ONE evaluation, so its suite, manifest, determinism and fingerprint checks do not mint separate loops. Evaluation 1 refused on rows appended already tombstoned; evaluation 2 refused on an archived review round the ledger did not cite; evaluation 3 passes the whole set on the final tree. Both refusals were fixed rather than waived, and the correction applied between evaluations 2 and 3 belongs to L2, where it took its own QA and its own delta review before the entire wave set re-ran — so no earlier wave evidence is stale. Trend is monotone: two refusals, then none, with no product defect raised by this loop at any point. |
| 4 | L6 — Owner-requested architect consultation | window 1 / cumulative 1 (roster addition recorded here) | `f1da5a6` + the current correction | 0 unresolved | 0 unresolved; 1 deferred row, narrowed | new: `overclaimed-served-text`, `placement-asserted-not-verified`. Recurring: `claim-measured-but-never-pinned` — THIRD instance, discharged structurally by the repo-wide ledger path-citation invariant, not patched again | **CONTINUE** | The gate raised three findings and one recommendation. All three findings are fixed in one batch; the recommendation is refuted on verified evidence. This is a fresh window, so no checkpoint was owed at the count — it is recorded because ADDING the loop is itself a checkpoint decision. `CONTINUE` rather than `CLOSE-CLEAN` because the batch mutates the tree and therefore owes its affected QA, a fix-only delta review and a full replay of the composite wave set BEFORE any closing decision; and because the one deferred row still has no placement, which is the owner's call and not a validation outcome. Trend: the consultation found real defects in what had already landed — including a false statement in the audit record itself — so the correct reading is that the previous close was premature on the RECORD, not that the loop is failing. |

Between rounds 2 and 3 an ADDITIONAL adversarial audit ran (six independent lenses over the
`b07babe..HEAD` source diff, each finding refuted-by-default by three skeptics, plus a
completeness critic): 12 raised, 3 refuted, 9 confirmed + 2 from the critic, all fixed before
round 3. It is recorded here as evidence, NOT as a roster loop: it is not a gate this repo
defines, it minted no budget, and its findings are dispositioned on the rows below like any
other. It is the reason round 3 was clean — it found the `region-bounded-on-the-wrong-axis`
critical defect, which round 1's own fix had introduced and which rounds 1–2 and live QA all
missed.

## Live evidence

Archive root: `docs/architecture/evidence/issue-156/` — a header-only `index.jsonl`
skeleton (no gate round is archived there yet; the Codex rounds append as they are
collected) plus `SHA256SUMS` covering every captured artifact, all tracked in git so the
archive is reproducible from a fresh clone rather than only present in this worktree.
Paths below are relative to that root.

Index authority, because the archive now holds three independent bodies of evidence:
`SHA256SUMS` (every file, hash-verified against the git index) and `index.jsonl` (one row per
collected gate round) are the authoritative indexes of the WHOLE archive. `captures/MANIFEST.json`
is narrower by design — it is the pre-implementation oracle run's own manifest and enumerates only
that run's 45 artifacts. It is not an incomplete index of `captures/`; `captures/stage1-r1/` is a
separate engagement with its own `MANIFEST.json`, and `commit-reviews/` holds the collector-written
Codex rounds. Recorded because a reader meeting `captures/MANIFEST.json` first could reasonably
misread its scope (raised by the QA agent at the close of the oracle engagement).

| Capture | Provenance source | Artifacts | Runtime proof | Status |
| --- | --- | --- | --- | --- |
| B1 `catcherrors → processcall` | frozen legacy builder at `b07babe`, re-emitted from a pristine `git archive` extraction and byte-identical | `captures/oracle-graphs/B1_*` | see C1–C6 below | **SATISFIED** |
| B2 `catcherrors → notify → processcall` | same; no committed golden existed for this shape — the gap #156 fills | `captures/oracle-graphs/B2_*` | see C1–C6 below | **SATISFIED** |
| Runtime controls | forced failure via `docker stop aderant-mssql`; child nonce channel is an INDEPENDENT counterparty (`cds-mock`), so the switch cannot confound it | `captures/runtime/C1..C6*.json` | C1/C2 success control: parent-target 1, child 0. C3/C4 failure: child ran once. C5 abort control: failing child under `wait=true`/`abort=true` turns the PARENT to ERROR. C6 restore reproduces C1. | **SATISFIED** |
| Sequential-region retry isolation | — | — | fail only the second connector; prove the source runs once, the second connector exhausts its retries, the first recovery leg stays cold, the second child runs once | **OWED** — architect plan §5 makes this blocking for the `ContinueNodeV1` region-boundary claim; it is part of the Stage-1 QA engagement |

Platform semantics the capture pinned, which #156 must preserve: a handled catch leaves the
parent reading COMPLETE despite a SEVERE try-leg error (read the counterparty, never the
record); the Notify `{1}` binds the real caught error at runtime; the platform labels the call
`standalone start data, no return data`; and the child gets its OWN `ExecutionRecord` with
`executionType="sub_process"`.

## Gate evidence

| Gate | Round | Run directory | Reviewed SHA | Outcome | Teardown |
| --- | --- | --- | --- | --- | --- |
| architect plan | 1 | `/tmp/cdx-gate-architect.H2YfQr` (removed) | n/a | `failed` — `declared_failed`, model/CLI mismatch | confirmed stopped |
| architect plan | 2 | `/tmp/cdx-gate-architect.VzdlSK` (removed after copy) | n/a | `completed`, attested | confirmed stopped |
| Stage-1 QA — pre-implementation live oracle | 1 | `agents/reports/2026-09-05-issue-156-oracle-r1.md`; artifacts under `captures/oracle-graphs/`, `captures/runtime/`, `captures/provision/`, `captures/deploy/` | `b07babe` (no source change) | **SATISFIED** — 45 artifacts, hash-indexed | n/a |
| Stage-1 QA — scoped live pass | 1 | `agents/reports/2026-09-06-issue-156-stage1-r1.md`; artifacts under `captures/stage1-r1/` | `9ab6789` | **PASS** — scenarios A/B/C/D, including the sequential-region retry-attribution control | n/a |
| Stage-2 repo commit review | 1 | `commit-reviews/cdx-review.BvI4Hx` | base `b07babe` → head `9ab6789`, clean tree | `completed`, collector exit 0; 3 findings | confirmed stopped |
| Stage-2 repo commit review | 2 | `commit-reviews/cdx-review.3LGJmM` | base `9ab6789` → head `3fd32fd`, clean tree | `completed`, collector exit 0; 1 finding (record-only) | confirmed stopped |
| Stage-2 repo commit review | 3 | `commit-reviews/cdx-review.SWQTjh` | base `3fd32fd` → head `5edf543`, clean tree | `completed`, collector exit 0; **no findings** | confirmed stopped |
| Architect implementation review | 1 | `architect-reviews/cdx-gate-review.LTjjBy` | tree at `ae03d1d` | `completed`, attested `ok:true`, verdict ISSUES FOUND | confirmed |
| Architect implementation review | 2 | `architect-reviews/cdx-gate-review.2RTrFA` | base `ae03d1d` → head `48d7ea0` | `completed`, attested `ok:true`, verdict ISSUES FOUND | confirmed |
| Architect implementation review | 3 (cap) | `architect-reviews/cdx-gate-review.y3Jowm` | base `48d7ea0` → head `f7b3687` | `completed`, attested `ok:true`, verdict ISSUES FOUND; ends the loop per the three-evaluation rule | confirmed |
| Stage-2 correction review (architect-cap closure) | 4 | `commit-reviews/cdx-review.0WcGHJ` | base `f7b3687` → head `3deba2b`, clean tree | `completed`, collector exit 0; **no findings** | confirmed stopped |
| Stage-1 QA — correction re-run | 2 | no report file (a clean run writes none); evidence in the agent's final report, instruments harvested to `.claude/agent-memory/boomi-qa-tester/harness/156-root-*.py` | working tree over `5939e866` | **PASS, 0 findings** — served identity agrees on code, pointer, message, remediation and contract-entry ids for both pairs; 36-row plan battery byte-identical with unmoved semantic and plan hashes; 48-cell blast-radius sweep moved 2 cells, both the same shape, no verdict flips; live A/B/D rig applied, deployed, executed and torn down on renera with `<shapes>` byte-identical to the r1 captures and every counterparty and sub-process execution cell identical; non-vacuity shown against two pre-fix trees | n/a |
| Stage-1 QA — compiler-mirror darkness proof | 3 | no report file (a clean run writes none); instruments harvested to `.claude/agent-memory/boomi-qa-tester/harness/156-mirror-*.py` and `156-compiler-route-instrumentation.py` | working tree over `5939e866` | **PASS, 0 findings** — 36-row plan battery and 48-cell sweep both unmoved; the compiler arm proved unreachable four ways: single-caller closure to the pipeline's unconditional re-parse, instrumented route measurement (14 mirror calls over 5 public routes, every one on the re-parsed object, zero caller-owned), an exhaustive 27,868-document reachability fuzz over the root alphabet (333 parsed, 0 mirror raises), and a delta grading showing all 10 shared refusals translatable with parser/mirror agreement on code AND pointer for 7 of 7 shapes | n/a |
| Stage-2 repo commit review | 5 | `commit-reviews/cdx-review.YM0rEW` | base `3deba2b` → head `6f1fc55`, clean tree | `completed`, collector exit 0; **no findings** — the complete unreviewed delta, so it covers the manifest commit `5939e86` as well as the structural correction | confirmed stopped |
| Composite wave gate | 3 | `wave-gate/wave.B6jltI` | wave SHA `ca2b051`, clean tree, base `b07babe` | **PASS**, exit 0 captured directly to a file (no pipeline): full non-KB suite 11642 passed / 19 skipped against a cap of 30, 11661 required nodes, 76 active goldens rendered twice in isolated children and byte-compared, plan-fingerprint seam checked on 2 cases | n/a |

Every row above is reproducible from the tracked archive: the run directory column names a path
under `docs/architecture/evidence/issue-156/`, each is indexed in `index.jsonl` with its collector,
status, reviewed SHA and teardown, and every file is hash-pinned in `SHA256SUMS`. No round is cited
that the archive does not hold, and no collected round is left uncited.

## The one deferred row, and the decision it is waiting on

*(Rewritten after an owner-requested architect consultation on 2026-09-07, whose factual premises
were then independently verified by five falsification-first checks. Three statements in the previous
version of this section were WRONG and are corrected below rather than quietly edited away: it
overstated what had been achieved, understated what the blocker requires, and promised a document
that did not exist. The consultation was a PLAN-kind gate turn, so it has no home in the review
archive — that contract covers commit reviews, implementation-review turns and wave-gate runs, and it
correctly refused a plan round. Its attested memo and attestation are at
`.codex/plans/issue-156-consultation.md` and `.codex/plans/issue-156-consultation.attest.json`, which
is where this repository keeps architect plans; both are under a gitignored root, exactly like the QA
reports cited above, so the SUBSTANCE is carried in this section rather than left to the artifact.)*

### What is now pinned, and what is genuinely left

The approved plan asks for two distinct things (`.codex/plans/issue-156.md`): prose lines 290-293 —
author canonical IR with matching **envelope metadata** and compare **complete emitted fixture
bytes** against the frozen file — and a table row at line 301 — move both corpus cases onto the
`_canonical_envelope_case` pattern, which is the COMPILE-GATED route.

The first is now discharged. `test_the_notify_golden_reproduces_as_a_complete_file` reproduces both
frozen goldens byte-for-byte as COMPLETE FILES, through the existing emitter and the existing
process-component materializer, with no capability change, no operation rebinding and no golden
regeneration. Four non-vacuity controls travel with it. The previous version of this section claimed
the headline criterion was satisfied by the shapes-level comparison alone; measured, that comparison
was leaving 579 bytes per golden unchecked — the component wrapper and all seven process-level
attributes, which is exactly the envelope metadata line 290 names. That claim was too favourable and
this correction is what makes it true.

What is left is line 301 alone: passing through `compile_process_ir_v1`. The pinned route
deliberately bypasses that gate, so the pin proves emission and materialization parity and proves
nothing about public authoring parity. This sentence is load-bearing — without it the pin will later
be misread as discharging the public-pipeline claim it does not cover.

### What the blocker actually requires, per golden

Both goldens drive a REST POST target and the canonical connector-call capability table registers
only read and partial-update intents for that family, so public compilation refuses both. The gap is
POST specifically, not REST writing generally: partial-update is already a registered write.

The previous version said a registered REST write intent would make the migration possible. Measured
against a synthetic capability row, that is true for ONE of the two:

- the single process-scoped handler golden — the row is necessary and SUFFICIENT; it then compiles
  and emits shapes byte-equal to the frozen file;
- the double-guard golden — the row is necessary and NOT sufficient. It is then refused on missing
  idempotency evidence for its retried region, and once that evidence is authored it is refused
  again on a profile mismatch at the map boundary. Three changes, not one.

The witness docstring carried the joint claim and has been corrected to state both cases separately,
with the measured diagnostic codes and the non-obvious reason the idempotency code is the one that
appears. Its failure is now labelled a trigger to RE-DECIDE, not a certificate that the migration is
possible.

### Where it goes — the open decision

The completion policy requires a deferred blocking-class row to name an ALREADY-FILED, sequenced
follow-up whose body carries the enumeration. This repo's standing rule is that only the owner
authorises a filing. So the row has its reason class and its enumeration but no issue number, and
until it has one this slice may LAND but may not CLOSE #156.

Issue #159 was considered as the home and does NOT qualify as-is. It is open, in the same milestone,
owns caller migration, carries survivor-golden byte-identity criteria and is sequenced before #160 —
but its body never names either of these two goldens, never mentions the corpus route, and its own
out-of-scope clause excludes the very act that unblocks the work. Its single notify reference is a
different golden pair, and the action there is retirement, not migration. Citing it unedited would be
paper compliance, not placement.

There is also an ownership gap worth deciding at the same time: no open issue currently owns adding
the REST write capability row, which is the single change that unblocks the simpler golden outright.

The three legal outcomes are set out in the closing report with the prepared issue body written out
in full, so whichever is chosen is one action, not an authoring exercise.

## Closing report

**Last validated tree.** `ca2b051` is the wave SHA — the last commit that changes anything the
composite gate measures. The commits after it add this report and the gate's own archived evidence,
which no gate reads as input. The landing SHA is re-validated in CI by the required non-KB check on
the exact commit that is fast-forwarded onto the integration branch, so the final tree is covered by
measurement rather than by the claim that documentation is harmless.

**Suite and manifests on the final tree:** 11642 passed, 19 skipped, 0 failed against a skip cap of
30; 11661 required nodes; 76 active goldens rendered twice in isolated children and byte-compared;
plan-fingerprint seam checked on two cases.

**Loops, in order, with their outcomes.**

| Loop | Evaluations | Outcome |
| --- | --- | --- |
| L1 — Stage-1 QA | 3 (r1 scoped live, r2 correction re-run, r3 compiler-mirror darkness proof) | PASS each time; r2 and r3 raised zero findings and wrote no report by the agent's own clean-run rule |
| L2 — Stage-2 repo commit review | 5 | rounds 1–3 closed at checkpoint 1 `CLOSE-CLEAN`; round 4 closed the architect cap; round 5 covered the structural correction and returned clean |
| L3 — Architect implementation review | 3 (the cap) | checkpoint 2 `CLOSE-CLEAN`; the review itself ended the loop |
| L4 — Composite wave gate | 3 | checkpoint 3 `CLOSE-CLEAN`; two refusals fixed, not waived |
| L5 — Terminal correction loop | 0 | never opened: no final non-blocking batch mutated the tree on its own |

The pre-implementation live oracle is evidence provisioning, not an evaluation, and debited no loop.
The six-lens adversarial audit between L2 rounds 2 and 3 is recorded as evidence for the same reason:
it is not a gate this repository defines, it minted no budget, and its findings carry ordinary
dispositions. It is nonetheless why round 3 was clean — it found the mutation-accounting defect that
round 1's own fix had introduced and that rounds 1–2 and live QA all missed.

**Residue.** Zero unresolved critical findings: every row deriving Critical is `fixed` and validated
on the current tree, none deferred and none closed over. One standard blocking-class row is deferred
— `ARCH-156-r2-06a`, reason class `blocked-by-mechanism` — and it is the only thing between this
slice and closure. Two rows are `not-validated` and say so plainly rather than counting as passes.

**Why the issue LANDS but does not CLOSE.** The completion policy requires a deferred blocking-class
row to name an already-filed, sequenced follow-up issue. This repository's standing rule is that only
the owner authorises a filing — the body is prepared and surfaced, never filed and then reported as
tracked. So the work lands and #156 stays open on exactly one decision, and the three legal outcomes
are set out below with the text each one needs.

**Correction, recorded rather than edited away.** An earlier version of this report stated that it
"carries the prepared issue body verbatim". It did not: the body existed only in a scratch file
outside the repository, and this report and the deferral section pointed at each other in a circle
around a document that was never written. That is a false claim in a durable artifact — the same
defect class this slice raised and fixed twice in code — and it is corrected here by writing the body
out, not by deleting the sentence. The sibling sweep over every other cross-reference in this ledger
that asserts a durable artifact found no second instance: every run directory, report path and
harness path named in the gate-evidence table resolves, and the archive scanner already enforces that
for the review rounds.

### The owner decision, and the three legal outcomes

**Outcome A — authorise an edit to issue #159's DESCRIPTION** (this repository puts issue updates in
the description, never in comments), adding the enumeration below. #159 is otherwise a good home: it
is open, in the same milestone, owns caller migration, carries survivor-golden byte-identity criteria
and is sequenced before #160. The edit must also reconcile its out-of-scope clause, which as written
excludes registering the capability row that unblocks the work.

**Outcome B — authorise filing a NEW sequenced issue** carrying the same enumeration. Under the
completion policy this is debt minting and is recorded as such.

**Outcome C — rule that the residue does not earn an issue.** Record the ruling here, correct the
row's disposition accordingly, and close #156 on that ruling. This is the cheapest legal path and it
deserves to be judged on its merits: what remains is one route to an artifact whose bytes are already
pinned, guarded by a test that fails the day the blocker lifts.

Independent of A/B/C: **no open issue currently owns adding the REST write capability row.** The
slice that owned connector vocabulary is closed; #159 excludes it; #156 defers it. That ownership gap
is an owner call and is best settled in the same pass.

### The prepared enumeration, verbatim

> **Title:** M12 follow-up — route the two notify goldens through the canonical corpus envelope
>
> **Reason class:** `blocked-by-mechanism`. **Parent epic:** #134. **Milestone:** M12.
> **Placement:** strictly after the change that registers a REST write intent in the canonical
> connector-call capability table; blocks nothing.
>
> **Why this exists.** #156 pinned the two legacy notify goldens as COMPLETE FILES, byte-for-byte,
> through parse → lower → emit → materialize
> (`tests/test_process_ir_notify_recovery.py::test_the_notify_golden_reproduces_as_a_complete_file`).
> That route bypasses `compile_process_ir_v1`. What is still owed is the plan's line-301 migration:
> re-pointing the two corpus cases in `tests/_wave_gate_golden_corpus.py` onto the
> `_canonical_envelope_case` pattern, which exercises normalization, public compilation and late
> binding as well.
>
> **The two cases**, by manifest id and fixture:
> 1. `golden-000059` — `tests/fixtures/golden_xml/try_catch_notify_dlq_document_cache.xml`,
>    corpus case `_case_try_catch_notify_dlq_document_cache`.
> 2. `golden-000005` — `tests/fixtures/golden_xml/connector_scoped_trycatch_notify_dlq_document_cache.xml`,
>    corpus case `_case_connector_scoped_trycatch_notify`.
>
> **Prerequisites, measured per case — they are NOT the same.**
> - `golden-000059`: a registered REST write intent is necessary and SUFFICIENT. With one it compiles
>   and emits shapes byte-equal to the frozen file. Re-point it as soon as the row lands.
> - `golden-000005`: the row is necessary and NOT sufficient. It is then refused on missing
>   idempotency evidence for its retried region, and once that evidence is authored it is refused
>   again on a profile mismatch at the map boundary. Three changes, not one.
>
> **Acceptance criteria.**
> - Both corpus cases render through `_canonical_envelope_case`, preserving their registry keys and
>   manifest ids, and both frozen fixtures still compare equal as COMPLETE FILES.
> - The frozen goldens are NOT regenerated — they stay the pre-#156 legacy builder's output, which is
>   what makes them an oracle rather than a photograph of the code under test.
> - The separate legacy differential is KEPT, so a change moving both the golden and the canonical
>   emitter together is still caught.
> - The manifest change is expressed as a tombstone-plus-append transaction, since payload fields are
>   immutable per row and a renderer cannot be re-pointed in place.
> - `test_the_notify_goldens_cannot_take_the_canonical_corpus_route_yet` is removed in the same pass —
>   it exists only to fail when this work becomes possible, and its per-golden prerequisites are the
>   checklist for doing it. Nothing else in the slice is waiting on anything.
