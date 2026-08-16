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
| **DC-165-1** | a guard whose failure branch cannot be reached, so it passes whether or not the property holds | the property the guard claims to prove | **1** — R1-1 | Fixed executably: the witness now runs AFTER the `sys.path` insert, asserts the probe module's file exists, and requires the blocker's OWN `_Blocked` subclass — a `ModuleNotFoundError` is now an explicit failure, not a pass. *(Non-vacuity witness, measured here: with `sys.meta_path.insert` replaced by `pass`, the test FAILS; before this correction it passed.)* |
| **DC-165-2** | a hand-typed count restated as fact about a tree that can contradict it | the tree itself (which modules consume the corpus) | **1** — R1-2 | Structural response applied at instance 1 rather than waiting for instance 2 (this class was PRE-ENUMERATED at instantiation): the count is REMOVED from all four sites and replaced by the invariant "no case input is defined in a test module", whose checkable forms are the corpus importing zero test modules and the import-blocked render test. |
| **DC-165-3** | a doc/comment claim about behaviour that no check enforces | the code the claim describes | **3** — R1-3, R1-7, R1-10 | Second instance reached in this batch ⇒ structural fix mandatory and applied: where the claim was about code, the CODE was changed to make it true (total recipe-arm dispatch; `copy.deepcopy` at the three notify sites) and the deep-copy contract became an executable invariant, `test_rendering_every_case_mutates_no_corpus_module_state`. Sibling sweep: every CONTRACT bullet in the corpus docstring re-checked against the code; the stale `_render_env` rationale in `scripts/wave_gate.py` corrected. *(Non-vacuity witness, measured here: an inert extra key added to the corpus's listener-chain table inside a renderer makes the new invariant FAIL.)* |
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

* **Supersession map** (a revision MERGES onto its original: cells the revision states
  win, cells it marks *(inherits)* keep the original's value, and the merged row is what
  the tally reads — the original is retained above unedited):
  `INH-RD-1a → INH-RD-1`.

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
