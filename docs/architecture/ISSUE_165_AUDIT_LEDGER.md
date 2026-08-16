# Audit ledger — issue #165 (M12.13 follow-up)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`.
Conventions inherited from #152's end state apply from row one; platform-behaviour claims
carry a provenance marker (`measured here` / `documented, not measured` / `assumption`).

## Baseline (Stage-1 step 0)

- Issue: #165 — invert golden-corpus authority so tests consume the registry
- Step-0 baseline: `da320eb08e5f5d9ba55a0d7b54ccbb4d28d01bbb`
- Slice kind: dark (tests + docs + fixtures only; no `src/` or `server.py` change)
- Artifact trust boundary: the slice CREATES AND OWNS the case definitions inside
  `tests/_wave_gate_golden_corpus.py`, the alias blocks in the thirteen consuming test
  modules, one new regression test, and this ledger + archive. It CONSUMES, unchanged:
  the committed golden bytes under `tests/fixtures/golden_xml/`, the committed JSON case
  fixtures (parity corpus, rich-control, error-handling, recipe-parity baselines,
  `examples/m11/`), both wave-gate manifests' existing rows, and the production builders.
- Expected defect classes (pre-enumerated): hand-enumeration shadowing a derivable
  authority; a second hand-copy of a case definition or invocation derivation surviving
  the inversion; import-spelling divergence across the bare/`src.` dual-module hazard.

## Deviation (owner-instructed, recorded before the first round)

The owner directed 2026-08-16 that this setup slice runs WITHOUT Codex: the Stage-2
review substitute is one or more independent fresh-context Claude reviewer agents
(multi-lens, read-only, findings processed under the receiving-code-review discipline),
the same substitute shape #171's closing review used on owner instruction. No Codex
attestation is claimed anywhere in this ledger, and the archive holds no collector run
directories; reviewer inputs/verdicts are archived as plain files under
`docs/architecture/evidence/issue-165/substitute-reviews/` and covered by the archive's
checksum file.

## Loop roster (fixed BEFORE the first correction)

1. Stage-1 QA — darkness proof for a dark slice: empty `src/` + `server.py` diff vs the
   step-0 baseline, PLUS the two refactor-specific differentials (all 60 active goldens
   render byte-identically before/after; the full pytest collection node list is
   byte-identical before/after except the one appended regression node).
2. Stage-2 substitute review — independent Claude reviewer agents per the deviation
   above, delta-scoped per round.
3. Composite wave gate — local `wave` run on the slice tip (full suite + every active
   golden twice + determinism + manifests). The WAVE-level integration review for the
   #165/#173/#172 wave runs once after #172 and is recorded in #172's ledger.
4. Terminal correction loop — ONLY via a recorded roster-addition checkpoint.

## Defect-class ledger (empty at instantiation)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-RD-1 | inherited seed — #152's re-decided deferral (issue #165 body; deferral rows live in the #152 ledger) | "`tests/_wave_gate_golden_corpus.py` obtains its cases by importing the thirteen golden-producing test modules and calling their module-level helpers. The architect plan for #152 specified the opposite direction … Removing or renaming an owning test, or one of its module-level helpers, makes an otherwise-active golden unrenderable." | Standard (deferral re-decided 2026-08-14) | *(gate fragility — loud failure, NOT one of the eight blocking classes; the issue body records "fragility rather than a correctness hole")* | *(directional dependency inversion — not an instance of a hand-copy class)* | Standard — anchor: no source critical label; lineage: the single window-exhausted deferral is CONSUMED, so this appearance must be fixed, refuted, or escalated | this slice's Stage-1.5 baseline commit (the tree this ledger arrives in) | `fixed` — the corpus now owns every case definition and imports zero test modules; the thirteen modules consume via alias blocks; acceptance criteria 1–5 discharged (see QA evidence below and the regression test named there). |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement).

## Stage-1 QA evidence (darkness proof + refactor differentials, recorded at Stage-1.5)

All measured here, 2026-08-16, on the working tree at the step-0 baseline
`da320eb08e5f5d9ba55a0d7b54ccbb4d28d01bbb` plus this slice's changes:

- Behavior surface untouched: `git diff da320eb08e5f5d9ba55a0d7b54ccbb4d28d01bbb --stat -- src/ server.py scripts/` → empty output (quoted at close in Final-tree validation).
- Golden-render differential: a `--render` request over all 60 active manifest rows was
  executed against the corpus BEFORE the refactor and AFTER it; the two id→sha256 maps
  are byte-identical (60/60). This is the load-bearing half of the darkness proof for a
  registry refactor.
- Collection differential: `pytest --collect-only -q` node lists before/after are
  byte-identical (9,790 lines) before the manifest transaction; the transaction then
  appends exactly one node,
  `tests/test_wave_gate_goldens.py::test_every_active_golden_renders_with_all_test_modules_unimportable`
  (manifest row `pytest-009792`, floors 9790 → 9791), verified by the gate's own
  `manifests --base` command.
- Acceptance criterion 3 regression: the new test renders every active golden to its
  committed bytes in a child process where every `test_*` module is unimportable, with an
  armed-blocker witness and a no-leaked-test-modules assertion. Mutation evidence: with a
  corpus factory temporarily re-pointed at a test-module helper, the test fails
  (measured here — RED observed, then the mutant reverted and the test is green again).
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
