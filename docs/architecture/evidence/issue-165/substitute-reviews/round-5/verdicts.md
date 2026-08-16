# Round-5 substitute review — charter and verdict (issue #165)

Reviewed delta: `62d6cd92c7437156d8b36806f9c1d0a3668cecaf..9cd689af64d813ec806d5f08ca8509d969631736`
(the round-4 correction only). One independent fresh-context Claude reviewer, read-only,
mutations run in throwaway `git worktree` checkouts. No Codex; see the Deviation section
of `docs/architecture/ISSUE_165_AUDIT_LEDGER.md`.

## Verdict

`VERDICT: ISSUES FOUND` — four P2 and nine P3 (ledger rows R5-1 … R5-13).

## Findings

- **P2 — the control derives its expectation from the function under test.** A two-line
  exclusion in the shared collector plus the reverted round-3 protective copy left 1257
  tests green: the circularity was moved one function upstream, not removed. (R5-1)
- **P2 — the control only distinguishes depth-1 from depth-2.** A depth-2 cap, or deleting
  either walker's sequence-recursion branch, passes while losing a real leak; 20 of 85
  nested members sit at depth ≥3. (R5-2)
- **P2 — "closes the memoising mutant" is false for the memo shape the comment names.** A
  cached accessor with its copy dropped leaves 171 tests green: the cache is not an
  uppercase module attribute and the watch set is snapshotted before the render. (R5-3)
- **P2 — every count the correction writes is the pre-correction measurement.** 24 of 60 is
  really 3 of 60; the flow-builder family contributes 60 not 41; container-carrying calls
  are 113 not 70. The bound now OVERSTATES the hole 8×, and it propagated into #174. (R5-4)
- P3 — the new type assertion is vacuous for exactly the types its comment names. (R5-5)
- P3 — the root collector still hand-copied the container-type tuple. (R5-6)
- P3 — the table-orphaning fix is the fourth recurrence of the defect it diagnoses. (R5-7)
- P3 — "restated precisely in all three places": the corpus CONTRACT block was untouched. (R5-8)
- P3 — "DC-165-1's cell now says exactly that": the cell was byte-identical. (R5-9)
- P3 — no round-4 archive; index `source_tip` two commits stale. (R5-10)
- P3 — instance ordinals not updated after the R3-1a reassignment. (R5-11)
- P3 — R4-2 records TWO dispositions where the policy requires exactly one. (R5-12)
- P3 — #174 body defects; R4-7 defers residue with no deferral record; `window-exhausted`
  spends a one-shot lineage where `blocked-by-mechanism` is the accurate class. (R5-13)

## Verified clean by this reviewer

R4-1's headline fix works (the top-level regression now FAILS naming 65 omitted members);
all six argument-side pins hold; R4-6 and R4-8 are correct; ledger append-only holds with
all 31 previously committed rows byte-identical and the declared class totals recomputing
correctly under supersession; CP-2's tier counts, evaluation accounting and outcome
eliminations are sound; the deferral to #174 is legal. All hard gates green at `9cd689a`:
goldens byte-identical to `da320eb`, node list = baseline + five, `manifests --base` ok at
9795, archive 10/10, full non-KB suite 9778 passed / 17 skipped via `ci --base`.
