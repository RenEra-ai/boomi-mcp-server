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
  six new tests, two appended revision rows in `ISSUE_171_AUDIT_LEDGER.md`, one template
  sentence, and this ledger + archive. It CONSUMES, unchanged: the committed ledgers'
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

Pointer-only; the reason class, placement and lineage live on the finding row and in the
filed issue.

- **INH-CR-2's archive half → #164.** Item 3.2's finding names two escapes: a committed
  one-step ledger deletion (fixed here) and a ledger+archive CO-deletion in one commit,
  which would also escape the attestation scanner's orphaned-archive assert. The second
  needs the scanner to walk history for archives, which is a different mechanism from the
  ledger-path walk this slice adds. Reason class `blocked-by-mechanism`; first deferral.
  *(Recorded at this slice's reconciliation; see the checkpoint that authorizes it.)*

## Evidence index

Archive root: `docs/architecture/evidence/issue-173/` with `index.jsonl` + `SHA256SUMS`
created in the same Stage-1.5 commit as this file. No collector run directories exist for
this slice (see the Deviation section); reviewer artifacts are archived under
`substitute-reviews/` in the batch that collects them.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
