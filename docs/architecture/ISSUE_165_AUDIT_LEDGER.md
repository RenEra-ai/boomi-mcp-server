# Audit ledger — issue #165 (M12.13 follow-up)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`.
Conventions inherited from #152's end state apply from row one; platform-behaviour claims
carry a provenance marker (`measured here` / `documented, not measured` / `assumption`).

## Baseline (Stage-1 step 0)

- Issue: #165 — invert golden-corpus authority so tests consume the registry
- Step-0 baseline: `da320eb08e5f5d9ba55a0d7b54ccbb4d28d01bbb`
- Slice kind: dark (tests + docs + fixtures only; no `src/` or `server.py` change)
- Artifact trust boundary: the slice CREATES AND OWNS the case definitions inside
  `tests/_wave_gate_golden_corpus.py`, the alias blocks in every consuming test module,
  the regression tests it adds, and this ledger + archive. It CONSUMES, unchanged:
  the committed golden bytes under `tests/fixtures/golden_xml/`, the committed JSON case
  fixtures (parity corpus, rich-control, error-handling, recipe-parity baselines,
  `examples/m11/`), both wave-gate manifests' existing rows, and the production builders.
- Expected defect classes (pre-enumerated): hand-enumeration shadowing a derivable
  authority; a second hand-copy of a case definition or invocation derivation surviving
  the inversion; import-spelling divergence across the bare/`src.` dual-module hazard.

## Deviation (owner-instructed, recorded before the first round)

The owner directed 2026-08-16 that this setup slice runs WITHOUT Codex: the Stage-2
review substitute is independent fresh-context Claude reviewer agents (multi-lens,
read-only, findings processed under the receiving-code-review discipline).

Precedent, cited so it is checkable rather than asserted: issue #171's body, section
**"Deviation recorded: substitute final gate review"**, records that its final
delta-scoped review (`589ed2c..da320eb`) ran as "an **independent Claude reviewer
workflow** (three finder lenses + adversarial verification + synthesis) instead of
Codex, **on the owner's explicit instruction**". That record lives in the issue, NOT in
`ISSUE_171_AUDIT_LEDGER.md` and NOT in `docs/architecture/evidence/issue-171/` — which
holds Codex collector runs only. A reviewer of this slice checked the #171 ledger and
archive, found no substitute-review evidence there, and correctly flagged the
uncited claim (row R1-8).

No Codex attestation is claimed anywhere in this ledger, and the archive holds no
collector run directories; reviewer inputs/verdicts are archived as plain files under
`docs/architecture/evidence/issue-165/substitute-reviews/` and covered by the archive's
checksum file.

## Loop roster (fixed BEFORE the first correction)

1. Stage-1 QA — darkness proof for a dark slice: empty `src/` + `server.py` diff vs the
   step-0 baseline, PLUS the two refactor-specific differentials (all 60 active goldens
   render byte-identically before/after; the full pytest collection node list is
   byte-identical before/after except the regression nodes this slice appends).
2. Stage-2 substitute review — independent Claude reviewer agents per the deviation
   above, delta-scoped per round.
3. Composite wave gate — local `wave` run on the slice tip (full suite + every active
   golden twice + determinism + manifests). The WAVE-level integration review for the
   #165/#173/#172 wave runs once after #172 and is recorded in #172's ledger.
4. Terminal correction loop — ONLY via a recorded roster-addition checkpoint.

## Defect-class ledger (classes assigned at reconciliation; counts derived from the rows)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| **DC-165-1** | a guard whose failure branch cannot be reached, so it passes whether or not the property holds | the property the guard claims to prove | **4** — R1-1, R2-1, R2-2, R2-5 | Fixed executably: the witness now runs AFTER the `sys.path` insert, asserts the probe module's file exists, and requires the blocker's OWN `_Blocked` subclass — a `ModuleNotFoundError` is now an explicit failure, not a pass. *(Non-vacuity witness, measured here: with `sys.meta_path.insert` replaced by `pass`, the test FAILS; before this correction it passed.)* |
| **DC-165-2** | a hand-typed count restated as fact about a tree that can contradict it | the tree itself (which modules consume the corpus) | **2** — R1-2, R2-4 | Structural response applied at instance 1 rather than waiting for instance 2 (this class was PRE-ENUMERATED at instantiation): the count is REMOVED from all four sites and replaced by the invariant "no case input is defined in a test module", whose checkable forms are the corpus importing zero test modules and the import-blocked render test. |
| **DC-165-3** | a doc/comment claim about behaviour that no check enforces | the code the claim describes | **5** — R1-3, R1-7, R1-10, R2-3, R2-6 | Second instance reached in this batch ⇒ structural fix mandatory and applied: where the claim was about code, the CODE was changed to make it true (total recipe-arm dispatch; `copy.deepcopy` at the three notify sites) and the deep-copy contract became TWO executable invariants — the consequence (`test_rendering_every_case_mutates_no_corpus_module_state`) and, after round 2 showed the first was green with the fix reverted, the antecedent (`test_no_case_factory_hands_module_state_to_a_helper_by_reference`), which is the one that actually pins it. Sibling sweep: every CONTRACT bullet in the corpus docstring re-checked against the code; the stale `_render_env` rationale in `scripts/wave_gate.py` corrected. *(Non-vacuity witness, measured here: an inert extra key added to the corpus's listener-chain table inside a renderer makes the new invariant FAIL.)* |
| **DC-165-4** | a test-only fixture migrated into the artifact whose purpose is to outlive test-module deletion | which corpus constants a registry case actually reads | **1** — R1-4 | Fixed with a complete sibling sweep: an AST pass enumerated every corpus module-level constant unreferenced by any case above the registry marker — exactly the two spare REST id constants of the flow-builder section and the source-stage id pair of the listener-chain section (the renderer tuple and the registry dict itself are the module API). All three returned to their owning test modules with the reason recorded inline. |

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-RD-1 | inherited seed — #152's re-decided deferral (issue #165 body; deferral rows live in the #152 ledger) | "`tests/_wave_gate_golden_corpus.py` obtains its cases by importing the thirteen golden-producing test modules and calling their module-level helpers. The architect plan for #152 specified the opposite direction … Removing or renaming an owning test, or one of its module-level helpers, makes an otherwise-active golden unrenderable." | Standard (deferral re-decided 2026-08-14) | *(gate fragility — loud failure, NOT one of the eight blocking classes; the issue body records "fragility rather than a correctness hole")* | *(directional dependency inversion — not an instance of a hand-copy class)* | Standard — anchor: no source critical label; lineage: the single window-exhausted deferral is CONSUMED, so this appearance must be fixed, refuted, or escalated | this slice's Stage-1.5 baseline commit (the tree this ledger arrives in) | `fixed` — the corpus now owns every case definition and imports zero test modules; the thirteen modules consume via alias blocks; acceptance criteria 1–5 discharged (see QA evidence below and the regression test named there). |
| R1-1 | Stage-2 substitute round 1, lenses A + B + C (independent Claude reviewers; no run dir — see the Deviation section) | "the new regression's 'non-vacuity witness' is inert; it passes whether or not the import blocker is armed" — the probe ran before the `sys.path` insert and named a `tests/patterns/` module, so it raised `ModuleNotFoundError` (an `ImportError` subclass) for a pure path reason | **P2** (all three lenses) | *(test-guard integrity — NOT one of the eight blocking classes; the acceptance property itself stayed proven by the leak scan)* | **DC-165-1** instance 1 | Standard — anchor: no source critical label | `7f20ea8` → this correction | `fixed` — witness moved after the path insert, probe file existence asserted, dedicated `_Blocked` exception required, `ModuleNotFoundError` made an explicit failure. Reported by three lenses = ONE instance per CLAUDE.md. Correctly caught: this is the repo's own #149/#152 "nothing checks the checker" shape, committed inside the very test written to prove a property. |
| R1-2 | Stage-2 substitute round 1, lens C | "'thirteen' is measurably wrong in every newly-authored assertion" — 11 modules consume the corpus; the pre-#165 registry imported 12 directly plus 1 transitively | **P2** | *(prose/docs — NOT a blocking class; but a ledger claim contradicted by the tree invalidates closure)* | **DC-165-2** instance 1 | Standard — anchor: no source critical label | `7f20ea8` → this correction | `fixed` — measured independently (11 alias-block modules; 12 `_mod()` targets at `da320eb`). The count is removed from all four sites rather than corrected to 11, per DC-165-2's structural response. |
| R1-3 | Stage-2 substitute round 1, lens B | "the recipe-arm table's 'the wrong arm fails immediately' comment overclaims" — only `"fanout"` discriminates; mutating `'sync_preset'` left all 60 goldens byte-identical, and the sync-preset branch hardcoded one recipe id | **P3** | *(prose + latent dispatch hazard — not a blocking class today; no golden can currently reach it)* | **DC-165-3** instance 1 | Standard — anchor: source label P3 | `7f20ea8` → this correction | `fixed` — dispatch made TOTAL (arm → (runner, recipe_id) table, unknown arm raises), comment rewritten to state what is and is not load-bearing. |
| R1-4 | Stage-2 substitute round 1, lens B | "three corpus constants are not case definitions and no golden pins them" — the two spare REST id constants and the source-stage id pair are read only by non-golden tests, and become dead corpus code when #159/#160 delete their owners | **P3** | *(gate-artifact hygiene — not a blocking class)* | **DC-165-4** instance 1 | Standard — anchor: source label P3 | `7f20ea8` → this correction | `fixed` — all three returned to their owning test modules after an AST sibling sweep proved the set is exactly these three. |
| R1-5 | Stage-2 substitute round 1, lens B | "now-dead aliases and imports left behind by the inversion" — 11 constants across four modules plus four unused imports, all newly dead in this delta | **P3** | *(dead code — not a blocking class)* | *(cleanup residue of the inversion, not a recurring mechanism)* | Standard — anchor: source label P3 | `7f20ea8` → this correction | `fixed` — each verified dead by occurrence count (definition line only) before removal, then the affected suite re-run. |
| R1-6 | Stage-2 substitute round 1, lenses A + B | "`_linear_with_map()` silently lost its byte anchor" — the golden test switched to the corpus case, whose config differs (stage keys `s/m/t` vs `source/transform/target`), leaving the helper ~15 other tests use pinned to nothing | **P3** | *(test coverage shape — not a blocking class; bytes unaffected)* | *(coverage shift, not a recurring mechanism)* | Standard — anchor: source label P3 | `7f20ea8` → this correction | `fixed` — the golden test now additionally asserts `_linear_with_map()` emits the SAME bytes, which re-pins the helper and states the stage-key-independence claim as a check instead of leaving two unrelated definitions. |
| R1-7 | Stage-2 substitute round 1, lens A | "the CONTRACT bullet 'Every renderer deep-copies shared inputs' is not true of three renderers" — the try/catch-DLQ notify dict (×2) and the archetype notify dict are passed by reference; aliasing verified `is`-identical | **P3** | *(prose vs code — latent, no live impact; pre-existing aliasing, newly co-located with the absolute claim)* | **DC-165-3** instance 2 | Standard — anchor: source label P3 | `7f20ea8` → this correction | `fixed` — `copy.deepcopy` added at all three sites, making the bullet true, AND the contract converted into the executable invariant `test_rendering_every_case_mutates_no_corpus_module_state` (manifest row `pytest-009793`). |
| R1-8 | Stage-2 substitute round 1, lens C | "the deviation's cited precedent is not supported by the tree" — `evidence/issue-171/` holds only Codex collector runs and `ISSUE_171_AUDIT_LEDGER.md` names a Codex closing gate | **P3** | *(audit-record integrity — NOT a blocking class)* | *(uncited claim; no recurring mechanism)* | Standard — anchor: source label P3 | `7f20ea8` → this correction | `severity-refuted` in part, `fixed` in part — the precedent IS in-tree-checkable, but in issue #171's BODY ("Deviation recorded: substitute final gate review"), not in its ledger or archive, which is exactly where the reviewer looked. The claim was true and unlocatable; the Deviation section now quotes and locates it, and records that the archive holds no substitute evidence. |
| R1-9 | Stage-2 substitute round 1, lens C | "`generated_at` is a synthetic `2026-08-16T00:00:00Z`" while the archived files were written at 04:52 — a provenance blemish in a slice whose thesis is measured-not-estimated | **P3** | *(audit-record integrity — NOT a blocking class)* | *(record hygiene)* | Standard — anchor: source label P3 | `7f20ea8` → this correction | `fixed` — replaced with the real UTC timestamp of the archive's creation. |
| R1-10 | Stage-2 substitute round 1, lens A (non-blocking note) | "`scripts/wave_gate.py` `_render_env`'s docstring still says the extra `tests/` and `tests/patterns/` path entries exist 'for the producers' sibling imports' — the corpus now has none" | **P3-equivalent** | *(prose vs code — the entries are inert, so behaviour is unaffected)* | **DC-165-3** instance 3 | Standard — anchor: no source critical label | `7f20ea8` → this correction | `fixed` — docstring corrected to state why each entry is really there and that no producer sibling import remains. Comment-only edit; `scripts/` carries no executable change (see the darkness note below). |

| INH-RD-1a | revision of INH-RD-1 (round-1 correction, per R1-2) | corrects the module count in INH-RD-1's DISPOSITION cell; INH-RD-1 is retained above unedited, and its verbatim summary — which quotes the issue — is untouched | *(inherits INH-RD-1)* | *(inherits)* | *(inherits)* | *(inherits)* | this correction | *(inherits INH-RD-1's `fixed` disposition)* — the count is withdrawn, not restated: **11** test modules consume the corpus, and the pre-inversion registry imported 12 directly plus 1 transitively, which is where the issue's "thirteen" came from. What the slice discharges is the INVARIANT — no case input is defined in a test module — whose checkable forms are the corpus importing zero test modules and the import-blocked render test. |

| R2-1 | Stage-2 substitute round 2 (fix-only, delta `7f20ea8..0b91c80`; independent Claude reviewer — see the Deviation section) | "the new 'executable invariant' does not pin the R1-7 fix" — reverting all three `copy.deepcopy` additions leaves the invariant, and all 526 tests in the affected modules, GREEN; it measures the consequence ("no state drifted") for today's builders, not the antecedent the contract asserts | **P2** | *(test-guard integrity — NOT a blocking class; the acceptance property itself is unaffected)* | **DC-165-1** instance 2 | Standard — anchor: no source critical label | `0b91c80` → this correction | `fixed` — REPRODUCED before fixing (the reverted-deepcopy mutant passed). A second test now measures the ANTECEDENT: every public corpus helper is wrapped during a full 60-case render and its arguments walked for objects that ARE module-level containers by identity. With the deepcopy reverted it now reports all five leak paths and FAILS; with the fix present it passes. Correctly caught, and the same class as R1-1 — a guard that cannot fail for the reason it claims. |
| R2-2 | Stage-2 substitute round 2 | "the invariant misses a REBIND" — it compares the objects captured at snapshot time, so a renderer rebinding a module-level table leaves it green while every later case sees the new one | **P3** | *(test-guard integrity — not a blocking class)* | **DC-165-1** instance 3 | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed` — the comparison now reads `getattr(corpus, name)`, so a rebind is drift. |
| R2-3 | Stage-2 substitute round 2 | "'every module-level container' overclaims" — the watch set excluded the registry dict for no stated reason, leaving it an unwatched blind spot; a fresh instance of the class this batch declares structurally fixed | **P3** | *(prose vs code — not a blocking class)* | **DC-165-3** instance 4 | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed` — the exclusion is removed (the reviewer verified deep-copying the registry works), so the watch set is now literally every module-level container and the sentence is true. |
| R2-4 | Stage-2 substitute round 2 | "R1-5's own count is contradicted by the tree, i.e. DC-165-2 recurring inside the batch that declares DC-165-2 structurally fixed" — the row says "11 constants across four modules"; the diff shows 14 across three | **P3** | *(audit-record integrity — NOT a blocking class)* | **DC-165-2** instance 2 | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed` by appended revision row **R1-5a**; R1-5 retained unedited. The reviewer is right that this is the same class recurring one commit after its structural fix — the fix removed hand-typed counts from the SHIPPED artifacts but I then hand-typed a new one into the ledger. R1-5a states the derivation instead of a tally. |
| R2-5 | Stage-2 substitute round 2 | "the witness's diagnostics are wrong and its 'not armed' branch is unreachable" — with the blocker disarmed the child exits on `witness probe was unreachable`, because the probe's transitive `boomi_mcp` import fails before the corpus sets up `sys.path` | **P3** | *(test-guard diagnostics — not a blocking class; the test still failed closed in every mutant)* | **DC-165-1** instance 4 | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed` — the child now establishes the FULL path (`src`, repo root, `tests`) before the witness, so the probe is genuinely importable when unblocked. *(Measured here: with the blocker disarmed the child now reports "the import blocker is NOT armed", the correct diagnosis.)* |
| R2-6 | Stage-2 substitute round 2 | "two shallow copies under a 'deep-copies' bullet" — the exception-catch dict and the target-stage id spread are copied shallowly; both dicts are flat scalars so behaviour is identical, but the word "deep" is literally false | **P3** | *(prose vs code — not a blocking class)* | **DC-165-3** instance 5 | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed` — the bullet now states the property that holds (no module-level container reaches production code by reference; copies are deep where they nest) and names both tests that assert it. |
| R2-7 | Stage-2 substitute round 2 | "the eleven new rows sit outside the Markdown table" — a blank line terminated the table, so the rows render as literal text | **P3** | *(audit-record legibility — not a blocking class; machine-parseable either way)* | *(formatting, no recurring mechanism)* | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed` — the blank line is removed and the rows join the table. Row BYTES are unchanged, so the append-only check still sees their first committed form. |
| R2-8 | Stage-2 substitute round 2 | "`import pytest` is still dead in the very import block R1-5 pruned" — pre-existing at `7f20ea8`; an incomplete sibling sweep for R1-5's own mechanism | **P3** | *(dead code — not a blocking class)* | *(sibling-sweep miss, same mechanism as R1-5)* | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed` — removed after confirming `pytest` appears nowhere else in that module. |
| R2-9 | Stage-2 substitute round 2 | "typo, 'the corpus corpus's'" | **P3** | *(prose — not a blocking class)* | *(typo)* | Standard — anchor: source label P3 | `0b91c80` → this correction | `fixed`. |
| R1-5a | revision of R1-5 (round-2 correction, per R2-4) | corrects the hand-typed constant count in R1-5's VERBATIM-adjacent disposition context; R1-5 is retained above unedited | *(inherits R1-5)* | *(inherits)* | *(inherits)* | *(inherits)* | this correction | *(inherits R1-5's `fixed` disposition)* — the tally is withdrawn rather than restated at a new number: the removal is DERIVABLE from the diff (`git diff da320eb..HEAD` on the four touched test modules) and from ruff's F401 delta, which is exactly four unused imports. Hand-typing "11" reintroduced DC-165-2 one commit after declaring it structurally fixed. |
| R1-7a | revision of R1-7 (round-2 correction, per R2-1) | corrects R1-7's claim that the contract "became an executable invariant"; R1-7 is retained above unedited | *(inherits R1-7)* | *(inherits)* | *(inherits)* | *(inherits)* | this correction | *(inherits R1-7's `fixed` disposition)* — the invariant R1-7 pointed at measures the CONSEQUENCE only, and was green with the deepcopy calls reverted, so it did not pin the fix. The antecedent is now pinned by `test_no_case_factory_hands_module_state_to_a_helper_by_reference`, verified RED against that exact mutant. |

* **Supersession map** (a revision MERGES onto its original: cells the revision states
  win, cells it marks *(inherits)* keep the original's value, and the merged row is what
  the tally reads — the original is retained above unedited):
  `INH-RD-1a → INH-RD-1`, `R1-5a → R1-5`, `R1-7a → R1-7`.

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement).

## Stage-1 QA evidence (darkness proof + refactor differentials, recorded at Stage-1.5)

All measured here, 2026-08-16, on the working tree at the step-0 baseline
`da320eb08e5f5d9ba55a0d7b54ccbb4d28d01bbb` plus this slice's changes:

- Behavior surface untouched: `git diff <baseline> --stat -- src/ server.py` → empty
  output at every commit of this slice. `scripts/` was empty at the Stage-1.5 baseline
  and carries exactly one COMMENT-ONLY hunk after the round-1 correction (R1-10, the
  `_render_env` docstring); no executable line in `scripts/` changed, so the slice stays
  dark in substance. Quoted at close in Final-tree validation.
- Golden-render differential: a `--render` request over all 60 active manifest rows was
  executed against the corpus BEFORE the refactor and AFTER it; the two id→sha256 maps
  are byte-identical (60/60). This is the load-bearing half of the darkness proof for a
  registry refactor.
- Collection differential: `pytest --collect-only -q` node lists before/after are
  byte-identical (9,790 lines) apart from the nodes this slice ADDS — two, after the
  round-1 correction: `…::test_every_active_golden_renders_with_all_test_modules_unimportable`
  (manifest row `pytest-009792`) and `…::test_rendering_every_case_mutates_no_corpus_module_state`
  (row `pytest-009793`), both in `tests/test_wave_gate_goldens.py`. Floors 9790 → 9792,
  each transaction verified by the gate's own `manifests --base` command.
- Acceptance criterion 3 regression: the new test renders every active golden to its
  committed bytes in a child process where every `test_*` module is unimportable, with an
  armed-blocker witness and two leak assertions — by module NAME and by resolved FILE
  (the file check added at round 1, so an `importlib` load of a test module's file under
  another name cannot hide the dependency). Mutation evidence, all measured here:
  (a) a corpus factory re-pointed at a test-module helper → the test FAILS;
  (b) the blocker itself replaced with `pass` → the test FAILS at the witness (before the
  round-1 correction this case passed, which is finding R1-1);
  each mutant reverted and the test green again afterwards.
- Acceptance criterion 4 measurement: bare corpus import 0.02–0.03 s wall; full render of
  all 60 cases in one child 0.43 s wall (raw outputs archived under
  `docs/architecture/evidence/issue-165/measurements/`, `measured here`).

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |
| **CP-1** — Stage-2 substitute review loop | 3 / 3 | `0b91c80` + dirty (the round-2 correction, uncommitted at decision time; committed as the next SHA) | **`CONTINUE`** | Written in flight, after the round-2 correction's owed validation and before the round-3 review. **Counting:** round 1 was three lenses run in parallel against one tree — one decision-bearing gate RESULT, so evaluation 1; the round-1 batched correction plus its validation is evaluation 2; the round-2 fix-only review plus this correction is evaluation 3. **Per-tier counts:** zero critical at every round (no finding landed in secrets/security, data loss, mutation accounting, or any other blocking class — every one is test-guard, prose, or audit-record integrity); standard 10 → 9. **Breadth:** round 1 spanned the corpus, four test modules, the spec, the ledger and the archive; round 2 is confined to `tests/test_wave_gate_goldens.py`, one corpus docstring bullet, and ledger rows — a real narrowing. **New/resolved/recurring classes (derived from the rows):** DC-165-4 resolved and not recurring; DC-165-2 recurred once (R2-4) *inside* the batch that declared it fixed, which is the honest black mark on this window; DC-165-1 accumulated three further instances, all of them the SAME guard being hardened rather than new guards failing; DC-165-3 gained two prose instances. **Trend:** highest unrefuted severity P2 → P2, but the round-2 P2 is a defect in the round-1 FIX rather than in the deliverable, and the deliverable itself (the inversion, the goldens, the node ids) has been byte-stable and reviewer-confirmed since `7f20ea8`. Unresolved count 0 at each round's close; affected-class breadth narrowing; nothing worsening; materially better on breadth. **Why not the other outcomes:** `CLOSE-CLEAN` is unavailable — the round-2 correction is owed a delta-scoped review it has not had, and the composite wave gate has not run on this tip. `DEFER-STANDARD-AND-PROCEED`/`-AND-CLOSE` would require enumerating residue into an already-filed issue, and there is none: every round-2 finding is fixed in this batch, so there is nothing to defer. `ESCALATE-OPEN` has no ground — validation is available and every finding had a concrete corrective action. **Named finite next correction:** none pending; the next action is validation, not mutation — round-3 review scoped to this correction delta, then the composite wave gate on the resulting tip. |

## Deferrals

None. This slice consumes no deferral and mints none; INH-RD-1's lineage was already
spent when #152 recorded the re-decision, which is why its disposition here is `fixed`.

## Evidence index

Archive root: `docs/architecture/evidence/issue-165/` with `index.jsonl` + `SHA256SUMS`
created in the same Stage-1.5 commit as this file. Contents: `measurements/` (criterion-4
raw outputs) and `substitute-reviews/` (reviewer inputs/verdicts, added in the batch that
collects them). No collector run directories exist for this slice (see the Deviation
section); the index stays header-only.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
