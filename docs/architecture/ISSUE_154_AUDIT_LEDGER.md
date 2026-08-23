# Issue #154 — M12.16 ProcessIR grammar and effect foundation (T2 + T8)

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Baseline SHA (`$BASELINE`) | `c41bcf085fcc0dcdc1efa5dde119c61932599175` |
| Branch | `codex/issue-154` |
| Branch point | `origin/dev` @ `c41bcf08` (tip of #177) |
| Baseline suite | 10264 passed, 17 skipped (10281 collected) — full non-KB, `-p no:randomly`, 849.50s |
| Baseline suite run | local `.venv/bin/python` 3.12, `PYTHONPATH=src`, before any edit |

The collected count 10281 equals the recorded floor from #177, so the floor is current on the
branch point.

## Evidence archive

Durable gate evidence for this slice is archived under `docs/architecture/evidence/issue-154/`.
Every attestation this ledger cites resolves there: the collector-written artifact, its
`attestation.json`, and the prompts the gate actually ran. The archive is instantiated with the
ledger (header-only `index.jsonl` + `SHA256SUMS`) so a citation can never point at a directory that
does not exist, and `tests/test_wave_gate.py::test_audit_ledger_attestations_have_durable_matching_evidence`
re-verifies the pairing on every run.

## Loop roster (enumerated in advance, before the first correction)

Per the shared-accounting rule, the roster is fixed here. A gate not on this list cannot mint a
loop mid-run; adding one is itself a recorded checkpoint decision.

| # | Loop identity | Authority | Scope | Window |
| --- | --- | --- | --- | --- |
| 1 | Stage-1 QA | `boomi-qa-tester` via the public MCP tool boundary | this slice | severity-aware checkpoint every 3rd evaluation |
| 2 | Stage-2 repo commit review | Codex detached review, `CLAUDE.md` §5 recipe | slice, then fix deltas | severity-aware checkpoint every 3rd evaluation |
| 3 | §6 architect implementation review | `/codex-claude:codex-issue` §6 gate (additive, impl-vs-plan) | slice, then fix deltas | **FIXED CAP: 3 evaluations** (owner rule 2026-08-22) |
| 4 | Composite wave gate | `scripts/wave_gate.py wave` | wave — suite, goldens ×2, fingerprint seam | severity-aware checkpoint every 3rd evaluation |
| 5 | Terminal correction loop | only if a final non-blocking batch mutates the tree | that batch | severity-aware checkpoint |

Declared order: **1 → 2 → 3 → 4**. Loop 3 is the additive gate the wrapping pipeline declares;
it starts a FRESH checkpoint window and spent inner-loop counts never block it. A mutation applied
at loop 3 or 4 replays the earlier gates it invalidated, in declared order.

The `/codex-issue` §3 architect DESIGN plan gate is not on this roster: it produces the plan, it
does not evaluate the implementation. Only the §6 implementation review is a roster gate.

## Finding ledger

One row per raw finding. Columns per the audit-record rule: source ID + verbatim summary, source
gate / run directory / attestation, original severity label, blocking class, defect class, derived
tier + anchor, affected SHA/delta, exactly one disposition.

| ID | Verbatim summary | Source gate / run dir | Orig. label | Blocking class | Defect class | Derived tier (anchor) | Affected SHA | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| QA-154-r1-01 | "an effect declaration that makes a blocking finding non-blocking can never compile" — `build_materialization_plan` takes no `capabilities` parameter and re-compiles strict, so a declaration validates clean at `plan` and fails at `compile`; the caller is strictly worse off than omitting it. `run_recipes(effect_declarations=…)` additionally had no caller anywhere. | Stage-1 QA loop, round 1, live through the public MCP tool boundary; report `agents/reports/2026-08-23-issue-154-grammar-effect-r1.md` | **High** | capability reachability | DC-154-A (server-built context threaded to SOME compile sites, authority = the set of compile entries a root passes through) | **Critical — anchor: the source gate labeled it High, and the tier rules derive Critical from a P0/P1/Critical/High source label. Not deferrable.** | `adff2fc` -> fixed | `fixed` — `build_materialization_plan` accepts and uses the resolved context; `_build_compile_time_plan` threads it; `_normalize_recipe_intent` forwards declarations to `run_recipes` so the recipe intent kind (which compiles inside normalization, before validation) can use the channel at all. Regression test is a behavioural spy over the core compile with a discriminating control, not a signature check. |
| QA-154-r1-02 | "a Python exception class name is served as a machine `cause_code`" — `_cause_codes_for` falls back to `type(exc).__name__`, so a `ProcessIRCompileError` whose `diagnostics` carry registered codes served the literal string `"ProcessIRCompileError"`. | Stage-1 QA loop, round 1 | Medium | machine-served schemas/contracts | DC-154-B (exception CLASS treated as provenance, authority = the exception's own registered diagnostics) — **second instance**, first was QA-153-r16-01 for pydantic | Standard — anchor: source label Medium; no critical class or anchor. | `adff2fc` -> fixed | `fixed` **structurally**: the rule is stated once — ask the exception for its own codes, in priority order, dedup preserving order, fall back to the class name only when it carries none — rather than adding a per-type branch. Widens what is reported, never what is refused; the fallback keeps its own test. |
| QA-154-r1-03 | "the served `return_documents` entry contradicts the widened grammar — and itself" — `ordering_facts[0]` read "Admitted in the root sequence only — not in a Branch path, a Decision arm, or a Try/Catch body" while the SAME served object's `placements` list carried `try_body/terminal`. | Stage-1 QA loop, round 1 | Medium | machine-served schemas/contracts | DC-154-C (hand-written placement prose, authority = `BODY_CAPABILITIES_V1`) | Standard — anchor: source label Medium. | `adff2fc` -> fixed | `fixed` via the structural fix below (shared with QA-154-r1-04). |
| QA-154-r1-04 | "the served `cache_put` rule is now false in a catch body — in three places" — the projection entry, the `ProcessIRV1` json_schema and the `CachePutNodeV1` docstring all still said a trailing `cache_put` belongs in a Branch path terminal. | Stage-1 QA loop, round 1 | Medium | machine-served schemas/contracts | DC-154-C (**second instance of the pair**) | Standard — anchor: source label Medium. | `adff2fc` -> fixed | `fixed`. **Structural fix (mandatory on the second instance):** placement prose is now DERIVED — `_derived_placement_fact` reads `BODY_CAPABILITIES_V1` and `_derived_trailing_cache_put_fact` reads the new single-authority trailing-cache-put terminals table in the models module (which also collapsed the last hand-copy of the trailing-cache_put rule). **Sibling sweep:** every hand-written `ordering_facts` string was swept; four name a placement slot, each verified against the matrix and allowlisted with its reason. **Non-vacuity witness:** planting an unreviewed placement sentence fails the projection build. **Coverage claim:** every node entry carries exactly one derived placement fact, tested in both directions against `body_placement_rows()`. The derived sentence is deliberately scoped to control bodies — the matrix does not govern the root sequence, and claiming otherwise would have re-created the same overclaim in generated form. |
| QA-154-r1-05 | "nothing anywhere pins `effect_declarations` out of the canonical payload" — `_normalized_payload` was referenced by zero test files, so the protection that keeps an absent field from rotating every existing hash was load-bearing and unwitnessed. | Stage-1 QA loop, round 1 | Medium | machine-served schemas/contracts | DC-154-D (an unwitnessed load-bearing omission) | Standard — anchor: source label Medium. | `adff2fc` -> fixed | `fixed` — three tests: absent field omits the key, an empty envelope omits it (normalising to `None` first), and a SUPPLIED declaration does enter it. The third is the control that keeps the first two from passing on a payload that never carries the key at all. |
| QA-154-r1-06 | "the new 'at least one connector_call' rule is unreachable dead code" | Stage-1 QA loop, round 1 | Low | non-blocking (dead code) | DC-154-E (a guard that cannot fire) | Standard — anchor: source label Low. | `adff2fc` -> fixed | `fixed` — removed, and the reason recorded where reachability is actually decided: the branch is entered only when `connector_call` is in `kinds`, and the terminal allowlist admits no `connector_call`, so a call in `kinds` is necessarily a call in `body`. Verified by measurement: `[connector_call]` alone still rejects with `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` from the terminal check. |
| QA-154-r1-07 | "the deliberate opacity of `defined_process_property_*` passes for a coincidental reason" — mutant M22 annotates the row as a `dpp` accessor and SURVIVES, because the covering test supplies a parameters dict lacking the key the mutant would read, so `derive_map_effect` returns `None` for a different reason. | Stage-1 QA loop, round 1 | Medium | non-blocking (guard coverage over a correctness decision) | DC-154-F (a guard whose subject returns the same value for two different reasons) — same shape the dispatch had already caught once in `derive_map_effect` | Standard — anchor: source label Medium. | `adff2fc` -> fixed | `fixed` — the REAL rows are now asserted opaque directly, plus a mutation witness that annotates each row AND supplies the parameter the annotation would read, so the fall-through reason is eliminated; under the mutant the map derives a `dpp` effect and the witness detects the difference. |
| QA-154-r1-08 | "your item-5 provenance row understates itself" — the INTERLEAVE half is attested by the pre-baseline legacy golden `set_properties_ddp_dpp_flow_sequence.xml`; only the PREFIX half is genuinely unattested. | Stage-1 QA loop, round 1 | Low | non-blocking (provenance record accuracy) | DC-154-G (a provenance claim that is wrong by being too pessimistic) | Standard — anchor: source label Low. | `adff2fc` -> fixed | `fixed` — verified by measurement (that golden, committed 2026-07-02, emits `[start, connectoraction, documentproperties, documentproperties, connectoraction, stop]`: property shapes between two calls, none before the first) and the row now splits the two halves with the correct standing for each. A provenance record is wrong when it is too pessimistic as well as when it is too generous. |
| CDX-154-r1-01 | "[P1] Resolve effects against the artifact selected for execution" — when a map is `reference_only`, or a create collision is planned as `reuse`, effect resolution inspects the caller's CANDIDATE config; a reference-only config has no `map_type` or mappings so it was classified pure/replay-safe even though apply binds an arbitrary live map. | Stage-2 Codex commit review, round 1, run dir `/tmp/cdx-review.F9iSJi`, attested `STATUS: completed` / `SCOPE: branch diff against c41bcf08 head=8be63be dirty=false` | **P1** | capability reachability (secondary: runtime behavior — a suppressed retry-safety or lineage error) | DC-154-A (effect context that does not describe the artifact that will execute) — **second instance**, first was QA-154-r1-01 | **Critical — anchor: the source gate labeled it P1, and the tier rules derive Critical from a P0/P1/Critical/High source label. Not deferrable.** | `8be63be` -> fixed | `fixed` — measured first: an empty config derived `((), (), True)`, pure AND replay-safe. Derivation now requires an explicitly recognized `map_type` (absence of a map body is absence of evidence, and this system reads absence as denial everywhere else) and refuses a substitutable artifact (`create` under `reuse`, since the plan may bind an existing component whose live content was never inspected); an `update` is not substitutable because its config IS applied. Tests cover both the hole and a control that still derives. |
| CDX-154-r1-02 | "[P2] Canonicalize function names before effect lookup" — the builder resolves via `get_function_family` (strip + lowercase); the raw dict lookup does neither, so a padded, upper-cased spelling of the sequential-value family is emitted successfully yet treated as opaque. | Stage-2 Codex commit review, round 1, same run dir | P2 | non-blocking (the divergence fails closed) | DC-154-B' (a hand-copy of a lookup rule whose authority is the builder's own resolver) | Standard — anchor: source label P2; no critical class or anchor. | `8be63be` -> fixed | `fixed`, with the reviewer's RATIONALE partly refuted and the finding upheld anyway. The stated consequence — "dropping its replay-unsafe effect and allowing it inside a retried region" — does not hold: an unknown family makes the WHOLE map opaque, the declaration goes inert, and every strict finding still fires, so the divergence failed closed. What is real is that two spellings of one lookup rule existed at all; the derivation now calls the builder's own resolver. Recorded because a fix accepted for the wrong reason is a fix nobody can re-derive. |
| CDX-154-r1-03 | "[P2] Do not classify defaulted map reads as strict" — a `dynamic_process_property_get` supplying `default_value` cannot fail, but the derivation always recorded a strict read, so a CORRECT declaration would make an otherwise valid flow fail with `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE`. | Stage-2 Codex commit review, round 1, same run dir | P2 | runtime behavior (a correct input rejected) | DC-154-H (a derivation that loses a distinction its consumer cannot recover) | Standard — anchor: source label P2. | `8be63be` -> fixed | `fixed` — verified that `StateEffectV1.reads` is a bare `(scope, name)` pair with no has-default flag, so a contracted read is unconditionally strict and the distinction cannot be recovered downstream. A defaulted get now records no read; the control test proves an undefaulted one still does, so the omission is about the default rather than about reads never being recorded. |
| CDX-154-r1-04 | "[P2] Export the declaration models from the public package" — the envelope and its six constituent models were absent from both `authoring_workflow.__all__` and `boomi_mcp.models`, so `from boomi_mcp.models import ProcessIREffectDeclarationsV1` raised `ImportError`. | Stage-2 Codex commit review, round 1, same run dir | P2 | machine-served schemas/contracts (the documented Python model surface) | DC-154-I (a published contract unreachable through its documented surface) | Standard — anchor: source label P2. | `8be63be` -> fixed | `fixed` — all seven exported from both, and the models-package export PIN updated in the same change (that pin exists so a widening of the public surface lands in a reviewable diff, and it caught this one). |
| CDX-154-r1-05 | "[P2] Keep the served cache-put rules internally consistent" — the generated ordering fact correctly allowed a trailing step-position `cache_put` in selected bodies while the SAME served entry's summary still said every step-position `cache_put` requires an immediate cache read. | Stage-2 Codex commit review, round 1, same run dir | P2 | machine-served schemas/contracts | DC-154-C (hand-written prose restating a rule it does not own) — **third instance of the pair**, after QA-154-r1-03/-04 | Standard — anchor: source label P2. | `8be63be` -> fixed | `fixed`. The structural fix from QA-154-r1-04 derived the ordering FACT but left the SUMMARY hand-written, so the entry contradicted itself for exactly the bodies this slice widened. The summary now states only the position-independent half and defers the body-dependent half to the derived fact, and a test asserts the two do not contradict. Recorded as a third instance because the structural fix was applied to one field of the entry and not to its sibling — the sibling sweep covered `ordering_facts` and should have covered `summary`. |
| QA-154-r2-01 | "the rule is wrong for `reference_only`" — `integration_builder` resolves a `reference_only` component to a reuse INDEPENDENT of `conflict_policy`, and `component_materialization_mode` already decides this before it looks at `action`; `{reference_only: true, map_type: "direct"}` therefore derived a pure, replay-safe effect under clone/fail/update for a component never read. | Stage-1 QA loop, round 2 (broad, delta `adff2fc..fa4b07d`); report `agents/reports/2026-08-23-issue-154-grammar-effect-r2.md` | Medium | capability reachability (secondary: runtime behavior) | DC-154-A (effect context that does not describe the artifact that will execute) — **third instance** | Standard — anchor: source label Medium; no critical class or anchor. Blast radius measured as zero today, masked by the map-profile wall rather than prevented. | `fa4b07d` -> fixed | `fixed` **structurally**: `_may_be_substituted` now ASKS `component_materialization_mode` instead of re-deriving its rule. My first attempt at this fix compared against the literal `"reuse"` while the constant is `"reuse_reference"`, so it matched nothing and changed nothing — caught by my own verification table before it shipped, and the constants are now imported rather than re-typed. Two spellings of one value is the same defect as two spellings of one rule, at a smaller scale. |
| QA-154-r2-02 | "`conflict_policy` reaches `_validate_processes` but not `recipes._compile_processes`, so a recipe intent's declared policy is ignored (pinned to `reuse`)" | Stage-1 QA loop, round 2 | Low | non-blocking (the pin is the conservative direction) | DC-154-A (same omission shape as QA-154-r1-01) | Standard — anchor: source label Low. | `fa4b07d` -> fixed | `fixed` — threaded through `run_recipes` -> `_compile_processes` -> the resolver, and `_normalize_recipe_intent` supplies the intent's own policy. Doing so tripped `test_run_recipes_exposes_no_seam_for_a_validation_policy`, whose substring check forbids any `*policy*` parameter; the guard was narrowed to a reviewed-exception list after verifying that `conflict_policy` reaches only the substitutability decision and that `validation_policy=None` stays pinned at both compile call sites. Both mutants hand-run: an exemption-shaped parameter is killed, and so is a stale allowlist entry. |
| QA-154-r2-03 | "the r1-01 fix is pinned at the *callee* only" — deleting `(effect_capabilities or {}).get(component_key)` at its call site in `build_artifact_descriptors` leaves 4041 tests green while the external-writer compile fails again (measured). | Stage-1 QA loop, round 2 | Medium | non-blocking (guard coverage over a fix) | DC-154-J (a fix pinned at the callee while the call site is unwitnessed) | Standard — anchor: source label Medium. | `fa4b07d` -> fixed | `fixed` — a test now drives the real compile entry with the resolver replaced by one returning a known context, and asserts the call site hands it onward; a paired CONTROL reproduces QA's deletion and proves the pin discriminates. QA's exact mutant was hand-run against the new pin: 1 failed under the mutant, 56 pass restored. A callee that accepts a parameter nobody passes is not a fix. |
| CDX-154-r2-01 | "[P1] Recognize the supported map_function type in derivation" — the new allowlist rejects the supported `map_type="map_function"` alias before inspecting `function_mappings`, so a valid declaration goes silently inert; legitimate writes then cannot satisfy lineage, and an impure family such as `sequential_value` inside a retry region loses `replay_safe=False`, degrading a retry-safety ERROR to a non-blocking opaque-effect warning. | Stage-2 Codex commit review, round 2, run dir `/tmp/cdx-review.IXdFvc`, attested `STATUS: completed` / `SCOPE: branch diff against 8be63be head=bcafbe1 dirty=false`; archived at `docs/architecture/evidence/issue-154/commit-reviews/cdx-review.IXdFvc/` | **P1** | capability reachability (secondary: runtime behavior — a downgraded retry-safety error) | DC-154-K (a hand-copied VOCABULARY whose authority is the map builders' own supported-map-type tuples) — the same duplicate-authority pair shape as DC-154-A/C, its **fourth** appearance in this slice | **Critical — anchor: the source gate labeled it P1, and the tier rules derive Critical from a P0/P1/Critical/High source label. Not deferrable.** | `bcafbe1` -> fixed | `fixed` **structurally**, and the finding was WORSE than reported: measurement showed the hand-written set was wrong in BOTH directions — it omitted `map_function` AND invented `profile`, which no builder supports. A third builder (`MapScriptBuilder`: `script`, `map_script`) was also unaccounted for. The vocabulary is now ASKED of `DirectMapBuilder`/`MapFunctionBuilder`; script maps fall through to opaque deliberately, because a script map's effect authority is the vetted registry and inspecting its config would establish nothing. Non-vacuity witness narrows a builder's supported types and proves the derivation follows, with a control on the alias that remains. |
## Checkpoint records

| # | Loop | Window / cumulative eval | SHA (dirty) | Per-tier counts | Breadth | Defect classes new/resolved/recurring | Trend evidence | Outcome | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| _(no checkpoint due — Stage-1 QA is at evaluation 1 of its window; a checkpoint is forced at the third)_ | | | | | | | | | |

## Validation evidence (chronological)

| # | Gate | Scope | Run directory / artifact | Attestation | Outcome |
| --- | --- | --- | --- | --- | --- |
| 0 | Baseline suite | full non-KB @ `c41bcf08` | local run, pre-edit | n/a (baseline, not a gate) | 10264 passed / 17 skipped |
| 4 | Stage-2 Codex commit review (loop 2, eval 2) | FIX DELTA — `--base 8be63be`, the two correction commits only (Critical scoping rule) | run dir `/tmp/cdx-review.IXdFvc`; collector attested `STATUS: completed`, `SCOPE: branch diff against 8be63be8ae8f5e1c73960485719dc5103609137e (8be63be) head=bcafbe1663cacb8a658642caff8230847a60192f dirty=false` | collector-confirmed teardown; archived | 1 finding (P1 -> Critical) |
| 3 | Stage-1 QA (loop 1, eval 2) | BROAD re-run — the fix delta touched `authoring/process_materialization.py`, which the policy names as requiring one; delta `adff2fc..fa4b07d` | `agents/reports/2026-08-23-issue-154-grammar-effect-r2.md` | live account `renera`, six live applies, platform readbacks byte-identical to round 1 | all 8 round-1 findings verified CLOSED; 3 new (2 Medium, 1 Low); suite 10388 passed / 17 skipped |
| 2 | Stage-2 Codex commit review (loop 2, eval 1) | slice — `--base c41bcf08`, whole branch diff | run dir `/tmp/cdx-review.F9iSJi`; collector attested `STATUS: completed`, `SCOPE: branch diff against c41bcf085fcc0dcdc1efa5dde119c61932599175 (c41bcf0) head=8be63be8ae8f5e1c73960485719dc5103609137e dirty=false` | collector-confirmed teardown | 5 findings (1 P1 -> Critical, 4 P2) |
| 1 | Stage-1 QA (loop 1, eval 1) | slice — 6 grammar positives, 14 adversarial neighbours, 9 effect cells, served-contract audit; live through the public MCP tool boundary | `agents/reports/2026-08-23-issue-154-grammar-effect-r1.md` | live account `renera`, 32 components created and deleted, platform readback graph-verified | 8 findings (1 High -> Critical, 5 Medium, 2 Low); full non-KB suite 10363 passed / 17 skipped at `adff2fc` |

## Pre-implementation measurements (dispatcher, main thread)

Recorded before any code was written, because two of them decide whether the architect's
central design decision is a structural fix or scope creep.

### M1 — the four control step unions (measured, not read)

```
LinearNodeV1            cache_get cache_put cache_remove data_process document_cache_retrieve
                        flow_control map_ref message set_ddp set_dpp
BranchLegStepV1         = LinearNodeV1 + connector_call
DecisionTrueArmStepV1   = LinearNodeV1 + connector_call   (identical to BranchLegStepV1)
DecisionFalseArmStepV1  = LinearNodeV1 + connector_call   (identical to BranchLegStepV1)
TryCatchBodyStepV1      = LinearNodeV1 + connector_call - data_process - flow_control
```

**Consequence.** Collapsing all four onto one `ControlBodyStepV1 = LinearNodeV1 | ConnectorCallNodeV1`
widens Branch legs and both Decision arms by **nothing**, and the Try/Catch body by **exactly**
`data_process` + `flow_control` — precisely in-scope item 1. The design is therefore the
structural fix, not a silent widening: the defect class is
**(hand-copied control-body step vocabulary, `LinearNodeV1` + `ConnectorCallNodeV1`)** with FOUR
instances, one of which had drifted.

### M2 — the catch body shares the widened union (recorded decision, not an accident)

`TryCatchBodyStepV1` is shared by BOTH bodies (`models/process_ir.py:1708,1734`), so the catch body
gains `data_process`/`flow_control` too. The issue text says "try-body union". This is admitted as a
deliberate consequence of an ALREADY-EVIDENCED design decision — `body_capabilities.py:145`,
"Both Try/Catch bodies share ONE step vocabulary — a caught document is an ordinary document" —
and not as a fail-closed widening on absent evidence. Surfaced to the reviewer rather than folded in
silently.

### M3 — legacy parity evidence for items 1 and 2, re-anchored at HEAD

The issue's line numbers came from `9711a9c` and have drifted. Re-confirmed at `c41bcf08`:
`process_flow_builder.py:1185-1213` hands the COMPLETE linear `flow` list to
`_emit_try_catch_shapes` / `_emit_connector_scoped_try_catch_shapes`. That list can contain
`flowcontrol` (`:970-980`) and `dataprocess` (`:1002-1012`), and its terminal entry is
`_terminal_flow_entry(config)` (`:1168`), which emits `returndocuments` when
`return_documents.enabled` is set. So the legacy builder demonstrably wraps all three shapes inside
a process-scoped try body today — the parity-oracle evidence for in-scope items 1 and 2.

**Provenance class:** legacy-oracle parity capture (a source causally independent of the ProcessIR
implementation under test), per the Stage-1 clean-room fixture rule.

---

**Commit note.** This ledger stays UNCOMMITTED until it carries at least one finding row.
`tests/test_wave_gate.py::test_audit_ledger_revisions_are_append_only_and_fully_declared` rejects a
COMMITTED ledger that parses zero rows (a parser regression that silently dropped a whole ledger is
the defect it guards), while deliberately exempting an uncommitted one — because the workflow
instantiates the ledger at Stage-1 step 0 and requires a green suite BEFORE the Stage-1.5 commit.
No prior ledger defers a finding to #154, so there are no `INH-*` seed rows to carry: like #152,
this slice inherits nothing. The first gate finding makes it committable.

## Recorded limitations (not introduced by this slice)

* **No `map_ref` compiles through the public authoring path at all.** `build_symbol_table` never
  sets `input_profile_ref`/`output_profile_ref` (0 occurrences) and `_check_map_pair` treats an
  absent profile as a mismatch by design, so no map-bearing root reaches emission. QA measured this
  IDENTICALLY at `c41bcf08`, `adff2fc` and `fa4b07d` — it is pre-existing and this slice neither
  caused it nor closes it. Consequence for #154: the `map_effects` declaration family has no
  non-inert production path today, and neither do `script_effects` (the vetted registry ships
  empty, deliberately). The channel, its authorities and its refusals are all exercised and tested;
  what is not exercised is a live end-to-end map effect, and that is blocked upstream.
* **The linear PREFIX of item 5** — a property step ahead of the flow's first connector call — has
  no independent oracle. Recorded per-shape in
  `tests/fixtures/process_ir/issue154/PROVENANCE.md`; the INTERLEAVE half is legacy-attested.
