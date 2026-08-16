# Audit ledger — issue #173 (M12.13 follow-up)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`.
Conventions inherited from #152's end state apply from row one; platform-behaviour claims
carry a provenance marker (`measured here` / `documented, not measured` / `assumption`).

## Baseline (Stage-1 step 0)

- Issue: #173 — audit-record self-checks: per-ledger coverage witness and archive-scanner fixtures
- Step-0 baseline: `0df53ff1367fe1bd273f5362864ed6a2aa1ab054`
- Slice kind: dark (tests + docs only; no `src/`, `server.py` or `scripts/` change)
- Artifact trust boundary: the slice CREATES AND OWNS the per-ledger coverage assertion,
  the two extracted helpers (`_archive_index_entries`, `_vanished_frozen_ledger_paths`),
  six new tests, two appended revision rows in `ISSUE_171_AUDIT_LEDGER.md`, two template
  paragraphs (the seeding rule this slice's own assertion requires, and the contiguity rule
  #165 landed), and this ledger + archive. It CONSUMES, unchanged: the committed ledgers'
  finding-row bytes, both wave-gate manifests' existing rows, the evidence archives, and
  every production module.
- Expected defect classes (pre-enumerated from #165's end state, so a second instance
  triggers structurally ON ARRIVAL): a guard whose failure branch cannot be reached; a
  doc/comment claim about behaviour that no check enforces; a hand-typed count restated as
  fact about a tree that can contradict it.

## Deviation (owner-instructed, recorded before the first round)

Same as #165 and on the same instruction: this setup slice runs WITHOUT Codex, with
independent fresh-context Claude reviewer agents as the Stage-2 substitute. Precedent and
its location are recorded in `ISSUE_165_AUDIT_LEDGER.md`'s Deviation section, which quotes
issue #171's body verbatim. No Codex attestation is claimed anywhere in this ledger, and
the archive holds no collector run directories.

**Loop-length discipline carried forward from #165 (its CP-3 and closing lessons).**
CLAUDE.md grants non-blocking residue — prose, comments, docstrings, historical counts,
audit-record integrity — exactly ONE batched correction pass that "never reopens a gate".
#165 spent six review rounds treating such findings as gate-reopening. This slice batches
the record work once and holds to that rule.

## Loop roster (fixed BEFORE the first correction)

1. Stage-1 QA — darkness proof for a dark slice: empty `src/` + `server.py` + `scripts/`
   diff vs the step-0 baseline, plus the mutation evidence recorded below (each fix is
   individually pinned by a witness that fails against the pre-fix code).
2. Stage-2 substitute review — independent Claude reviewer agents per the deviation above,
   delta-scoped per round.
3. Composite wave gate — local `wave` run on the slice tip.
4. Terminal correction loop — ONLY via a recorded roster-addition checkpoint.

## Defect-class ledger (classes assigned at reconciliation; counts derived from the rows)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| **DC-173-1** | a guard whose failure branch cannot be reached, so it passes whether or not the property holds | the property the guard claims to prove | **4** — INH-AR6-3, INH-AR6-4, INH-CR-3, R1-1 | Every fix in this slice ships a witness that FAILS against the pre-fix code, verified by mutation and recorded in the QA evidence below; the inherited findings are themselves instances of this class, which is why the slice exists. Pre-enumerated at instantiation from #165's end state, so the second instance triggered structurally on arrival: the response is that no fix here is accepted without its mutation witness. |
| **DC-173-2** | an unpinned hand-copy of a fact whose authority lives elsewhere | the single definition the copy shadows | **3** — INH-CR-4, R1-4, R1-5 | Structural at instance 2: the finding-id shape is now ONE module constant (`_FINDING_ID_RE`) with the revision form derived from it, replacing three hand-copies; the two DC-16 ordinals were corrected by derivation from the class table rather than by re-typing; and the stale "#171's archive has 98 tracked paths" was re-measured (146). Sibling sweep: every regex and count this slice touches now cites or derives its authority. |
| **DC-173-3** | a doc/record claim about behaviour that no check enforces, or that the tree contradicts | the code or tree the claim describes | **3** — INH-CR-5, R1-2, R1-6 | Corrected in place: the rename comment names both authorities, the void deferral is withdrawn against its measurement, and the trust-boundary and template claims are restated to what actually shipped. |

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-AR6-3 | inherited seed — `ISSUE_171_AUDIT_LEDGER.md` row AR6-3, deferred at #171's CP-7 | "the append-only check's per-ledger coverage is unwitnessed" — `_finding_rows()` covers #152's rows as well as #171's, but the coverage is not ASSERTED per ledger: restoring the old delimiter-count parser would make #152 contribute zero rows while the test still passes, because #171 alone satisfies the global `checked` witness | **P3-equivalent** | *(audit-record integrity — NOT one of the eight blocking classes)* | *(inherited; a guard whose coverage is unasserted — see the class table once reconciled)* | Standard — anchor: no source critical label; lineage: FIRST deferral was `window-exhausted`, so this appearance must be fixed, refuted, or escalated | this slice's Stage-1.5 baseline commit | `fixed` — a per-ledger `assert parsed` now sits immediately after the parse, so a parser change that drops an entire ledger fails there instead of hiding behind another ledger's revisions; and `test_finding_rows_parses_both_committed_row_shapes_and_excludes_class_rows` pins both committed row shapes, the `DC-*` exclusion (with an assertion that `DC-16` really does match the id shape, which is what makes the exclusion load-bearing), first-occurrence-wins, and the too-few-cells case — all in memory, independent of what happens to be on disk. |
| INH-AR6-4 | inherited seed — `ISSUE_171_AUDIT_LEDGER.md` row AR6-4, deferred at #171's CP-7 | "two archive-scanner branches have no non-vacuity witness" — the `-z` enumeration and the mode-`120000` refusal are both correct but unexercised: #171's archive has 98 tracked paths, zero containing whitespace and zero symlinks, so a regression in either branch would go unnoticed | **P3-equivalent** | *(audit-record integrity — NOT a blocking class)* | *(inherited; an unexercised branch)* | Standard — anchor: no source critical label; lineage: FIRST deferral was `window-exhausted`, so this appearance must be fixed, refuted, or escalated | this slice's Stage-1.5 baseline commit | `fixed` — the index scan is extracted as `_archive_index_entries(repo_root, prefix)` so fixtures can drive it, and two temp-repo tests supply the cases the repository does not: a whitespace-containing archive path (which also reproduces the defect `-z` prevents, by showing the whitespace-split form losing the path), and a tracked mode-`120000` entry built with `hash-object` + `update-index --cacheinfo` under `core.symlinks=false`, where the worktree probe is blind and only the index sees it. |
| INH-CR-1 | inherited seed — issue #173 item 3.1, from #171's substitute closing review | "Case-only ledger rename is invisible to the frozen-path invariant on case-insensitive filesystems" — `git ls-files` under `core.ignorecase=true` still lists the original casing AND `is_file()` resolves case-insensitively, so both authorities report "present" | **P3-equivalent** | *(audit-record integrity — NOT a blocking class; CI's enforcement venue is case-sensitive and unaffected)* | *(a membership test standing in for an identity test)* | Standard — anchor: no source critical label; lineage: first deferral | this slice's Stage-1.5 baseline commit | `fixed` — the committed spelling must now appear in the GLOB's own output, not merely resolve through it. *(Measured here: probed in a throwaway repo — under `core.ignorecase=true` a case-only rename leaves `git ls-files` reporting the original spelling and `is_file()` true, while the glob returns the new spelling only.)* Witness: `test_frozen_ledger_invariant_sees_a_case_only_rename`, which fails against the membership-only form. |
| INH-CR-2 | inherited seed — issue #173 item 3.2 | "Committed one-step destruction is outside the invariant's domain" — the frozen set re-bases on HEAD each commit, so a single commit deleting a ledger escapes the invariant itself | **P3-equivalent** | *(audit-record integrity — NOT a blocking class; pre-existing at every earlier accepted base)* | *(a frozen set re-derived from the tree it is meant to freeze)* | Standard — anchor: no source critical label; lineage: first deferral | this slice's Stage-1.5 baseline commit | `fixed` — the frozen set is now every ledger path this history has EVER added (`git log --diff-filter=A` over HEAD's ancestry, not `--all`, which would freeze paths from abandoned branches). *(Measured here before relying on it: the ever-existed set equals the current set exactly — 152, 165, 171 — so no allowlist is needed, and the walk costs 0.15 s over 1,425 commits.)* Witness: `test_frozen_ledger_invariant_sees_a_committed_one_step_deletion`, which fails against the HEAD-only form. The archive half of the co-deletion case is enumerated in the issue and is NOT closed here — see the Deferrals section. |
| INH-CR-3 | inherited seed — issue #173 item 3.3 | "Unchecked `git ls-tree` returncode when deriving the frozen set — a solitary failure of that subprocess empties the set and passes the invariant vacuously" | **P3-equivalent** | *(audit-record integrity — NOT a blocking class; partially mitigated by the shallow-repository assert)* | *(a guard whose failure branch cannot be reached)* | Standard — anchor: no source critical label; lineage: first deferral | this slice's Stage-1.5 baseline commit | `fixed` — every git query behind the frozen set now runs through `_git_or_fail`, which asserts `returncode == 0` and reports the stderr; the sibling `ls-files` call and the new history walk are covered by the same helper, not just the `ls-tree` call the finding named. Witness: `test_frozen_ledger_invariant_refuses_a_broken_git_query`. |
| INH-CR-4 | inherited seed — issue #173 item 3.4 | "Two hand-typed DC-16 inline ordinals off by one in `docs/architecture/ISSUE_171_AUDIT_LEDGER.md` (RH-2 says 'instance 8', AR6-5 says 'instance 7'; the derived table's 7 members are correct) — the same hand-tally shape RG-1a corrected for DC-12, committed in the very row recording that correction" | **P3-equivalent** | *(audit-record integrity — NOT a blocking class)* | *(a hand-typed ordinal restated rather than derived)* | Standard — anchor: no source critical label; lineage: first deferral | this slice's Stage-1.5 baseline commit | `fixed` by APPENDED revision rows `AR6-5a` and `RH-2a` in #171's ledger, with both originals retained unedited and the supersession map extended in the same commit. Verified against the derived table's chronology: applying `AR3-4a` removes AR3-4 from the class, which is what shifts AR6-5 to 6 and RH-2 to 7. |
| INH-CR-5 | inherited seed — issue #173 item 3.5 | "Comment imprecision in the invariant's rationale: 'any rename leaves its source path missing from the worktree' understates the untracked-copy variant, which leaves the source present in the worktree and missing from the INDEX; the code checks both, the prose names one" | **P3-equivalent** | *(prose vs code — NOT a blocking class)* | *(a doc claim narrower than the code it describes)* | Standard — anchor: no source critical label; lineage: first deferral | this slice's Stage-1.5 baseline commit | `fixed` — the comment now names both authorities, says which variant breaks which, and records that a case-only rename breaks neither on a case-insensitive filesystem and is caught by the spelling comparison instead. |
| R1-1 | Stage-2 substitute round 1 (independent Claude reviewer, delta `0df53ff..a996b4d`; no run dir — see the Deviation section) | "the per-ledger `assert parsed` deadlocks new-ledger instantiation" — the workflow instantiates a ledger at step 0 and requires a green suite BEFORE the Stage-1.5 commit, so a freshly instantiated ledger could never be validated, so it could never be committed; and `ISSUE_152_AUDIT_LEDGER.md` has zero `INH-*` rows, a committed counterexample of a slice with nothing to inherit | **P1** | **capability reachability** — a required gate made unsatisfiable for every future slice | **DC-173-1** instance 4 | **Critical** — anchor: source label P1. Fixed and validated, never deferred. | `a996b4d` → this correction | `fixed` — REPRODUCED first (instantiating a fresh ledger from the template, with its archive skeleton, made the suite RED at the tip and green at the baseline). The assertion is now scoped to ledgers present in HEAD's tree, exactly like the first-appearance walk's existing historyless exemption three assertions later: a parser regression that drops a COMMITTED ledger still fails, while a brand-new one is exempt until it is committed. Correctly caught — this is the same new-ledger-QA deadlock class `e16b537` fixed one round earlier in #171, and my seeding convention could not close it because it is a convention, not an enforcement. |
| R1-2 | Stage-2 substitute round 1 | "the slice's only deferral is invalid on three counts" — it defers to #164, which does not mention ledgers, archives or the scanner; its justification is contradicted by an in-slice measurement; and it cites a checkpoint while the Checkpoints table is empty | **P2** | *(audit-record integrity — NOT a blocking class, but any of the three alone invalidates closure)* | **DC-173-3** instance 2 | Standard — anchor: no source critical label | `a996b4d` → this correction | `fixed` — the deferral is WITHDRAWN, not re-pointed. Independently reproduced: co-deleting `ISSUE_165_AUDIT_LEDGER.md` and `evidence/issue-165/` in one commit at the slice tip FAILS the frozen-path invariant, because a co-deletion necessarily deletes the ledger and the walk added here already covers it. CLAUDE.md voids a deferral its own slice's measurement contradicts, so the case is `fixed`; the slice now defers nothing. |
| R1-3 | Stage-2 substitute round 1 | "the history walk's git query is under-specified; two escapes remain" — unlike its sibling queries it uses neither `-z` (so git QUOTES non-ASCII paths and the anchored regex misses them) nor `--no-renames` (so a ledger created by renaming an in-pathspec file never enters the frozen set) | **P3** | *(capability reachability of the invariant — not a blocking class; both escapes need a deliberate act)* | **DC-173-2** instance 4 | Standard — anchor: source label P3 | `a996b4d` → this correction | `fixed` — both flags added. *(Measured here: without `--no-renames`, a ledger created by `git mv` and then deleted returns an EMPTY vanished set; with it, the deletion is reported.)* Correctly caught, and the sharper half is that this is the same `-z`-vs-naive-parse class the slice's own item-2 fixture exists to pin, reintroduced in the code added beside it. |
| R1-4 | Stage-2 substitute round 1 | "the whitespace fixture does not actually pin `-z`" — replacing `-z` with `.splitlines()` leaves all three tests green, because `git ls-files` does not quote spaces; the `-z` branch's real justification (git quotes non-ASCII paths) stays unwitnessed, and the naive-split reproduction is tautological | **P3** | *(test-guard integrity — not a blocking class)* | **DC-173-1** instance 5 | Standard — anchor: source label P3 | `a996b4d` → this correction | `fixed` — the fixture now also commits a non-ASCII archive path and asserts git quotes it without `-z`. *(Measured here: with `-z` replaced by `.splitlines()` the fixture now FAILS, where before it passed.)* |
| R1-5 | Stage-2 substitute round 1 | "the finding-id regex is hand-copied into the new test" — the same pattern now appears verbatim at three sites, so a widened parser leaves the fixture asserting a stale shape | **P3** | *(an unpinned hand-copy — not a blocking class)* | **DC-173-2** instance 5 | Standard — anchor: source label P3 | `a996b4d` → this correction | `fixed` STRUCTURALLY at the second instance, as the rule requires: `_FINDING_ID_RE` is one module constant, `_REVISION_ID_RE` is derived from it rather than written again, and all three sites now reference them. |
| R1-6 | Stage-2 substitute round 1 | "two stale hand-typed measurements" (#171's archive has 146 tracked paths, not 98; the walk covers 1,431 commits, not 1,425) and "two small record inconsistencies" (the trust boundary says one template sentence where two paragraphs shipped; the Defect-class table is empty while every row is disposed) | **P3** | *(audit-record integrity — not a blocking class)* | **DC-173-3** instance 3 | Standard — anchor: source label P3 | `a996b4d` → this correction | `fixed` — both counts re-measured and corrected, the trust boundary restated to what shipped, and the Defect-class table populated so the second-instance check can actually be run from the record. Exactly the class this ledger PRE-ENUMERATED at instantiation, arriving as predicted. |
| INH-CR-2a | revision of INH-CR-2 (round-1 correction, per R1-2 and R1-6) | corrects the commit count and withdraws the deferral pointer; INH-CR-2 is retained above unedited | *(inherits INH-CR-2)* | *(inherits)* | *(inherits)* | *(inherits)* | this correction | *(inherits INH-CR-2's `fixed` disposition)* — two corrections. **(1)** The walk covers **1,431** commits at the baseline, not 1,425 (`git rev-list --count 0df53ff`); the ~0.1 s timing stands. **(2)** INH-CR-2's closing sentence says the archive half of the co-deletion case "is NOT closed here"; it IS closed here, measured — co-deleting a ledger and its archive in one commit fails the frozen-path invariant, because a co-deletion necessarily deletes the ledger. The deferral that sentence pointed at is withdrawn as void (see the Deferrals section); the slice defers nothing. |

* **Supersession map** (a revision MERGES onto its original: cells the revision states
  win, cells it marks *(inherits)* keep the original's value, and the merged row is what
  the tally reads — the original is retained above unedited):
  `INH-CR-2a → INH-CR-2`.

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement).

## Stage-1 QA evidence (darkness proof + mutation witnesses, recorded at Stage-1.5)

All measured here, 2026-08-16, against the step-0 baseline `0df53ff`:

- Behavior surface untouched: `git diff 0df53ff --stat -- src/ server.py scripts/` → empty.
- Every code fix is individually pinned, each mutant reverted afterwards:
  - HEAD-only frozen set (drop the history walk) → `test_frozen_ledger_invariant_sees_a_committed_one_step_deletion` FAILS.
  - Membership-only (drop the byte-exact spelling comparison) → `test_frozen_ledger_invariant_sees_a_case_only_rename` FAILS.
  - Unchecked git (restore the silent-empty behaviour) → `test_frozen_ledger_invariant_refuses_a_broken_git_query` FAILS.
- Manifest transaction: six nodes appended (`pytest-009798`…`pytest-009803`), floors
  9796 → 9802, verified by `wave_gate.py manifests --base 0df53ff`.

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

## Deferrals

**None.** An earlier revision of this section deferred "INH-CR-2's archive half" to #164 on
the ground that a ledger+archive CO-deletion in one commit needs the scanner to walk
history for archives. **That ground is false, and the measurement is in the record**
(row R1-2): co-deleting `ISSUE_165_AUDIT_LEDGER.md` and `evidence/issue-165/` in one commit
at this slice's tip FAILS `test_audit_ledger_revisions_are_append_only_and_fully_declared`
with "committed ledger paths are FROZEN … ['docs/architecture/ISSUE_165_AUDIT_LEDGER.md']",
because a co-deletion necessarily deletes the ledger and the ledger-path walk added here
already covers it. Archive-alone deletion is caught by the pre-existing "every ledger owns
an archive" assertion. CLAUDE.md voids a deferral whose justification an in-slice
measurement contradicts; the deferral is therefore withdrawn rather than re-pointed, and
the case is `fixed`, not deferred. The deferral was also defective in two further ways the
reviewer named — #164 does not cover this subject at all, and it cited a checkpoint that
did not exist — either of which alone would have invalidated closure.

## Evidence index

Archive root: `docs/architecture/evidence/issue-173/` with `index.jsonl` + `SHA256SUMS`
created in the same Stage-1.5 commit as this file. No collector run directories exist for
this slice (see the Deviation section); reviewer artifacts are archived under
`substitute-reviews/` in the batch that collects them.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
