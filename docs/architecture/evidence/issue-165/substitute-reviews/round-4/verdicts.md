# Round-4 substitute review — charter and verdict (issue #165)

Reviewed delta: `06976a0d9ef856edaa2b661cceef9ab315f47308..62d6cd92c7437156d8b36806f9c1d0a3668cecaf`
(the round-3 correction only). One independent fresh-context Claude reviewer, read-only,
mutations run in throwaway `git worktree` checkouts. No Codex; see the Deviation section
of `docs/architecture/ISSUE_165_AUDIT_LEDGER.md`.

## Verdict

`VERDICT: ISSUES FOUND` — two P2 and seven P3 (ledger rows R4-1 … R4-9).

## Findings

- **P2 — the structural fix ships no control for the defect that motivated it.** Regressing
  the watch-set construction to top-level-only left both new controls and the antecedent
  guard green, because the control built its own map from the walker directly. (R4-1)
- **P2 — the stated residual bound understates the hole ~8×.** Only arguments were
  inspected, never return values nor objects handed straight to production; two mutants (a
  module-level config passed to a builder, a memoising accessor) left the suite green. (R4-2)
- P3 — "a committed negative control per guard" unmet for the drift invariant. (R4-3)
- P3 — the commit fixing table-orphaning blank lines orphaned its own six new rows. (R4-4)
- P3 — the vacuity floor had 3.5× slack and would not have caught the round-3 defect. (R4-5)
- P3 — narrowing the wrapper filter to plain functions silently drops decorated helpers. (R4-6)
- P3 — dead parameter; "every container reachable" overstates what the walkers do. (R4-7)
- P3 — the watch map held bare ids with no strong reference (phantom-leak risk). (R4-8)
- P3 (observation) — R3-1's defect class belongs to DC-165-1, not DC-165-3. (R4-9)

## Verified clean by this reviewer

The round-3 mutant is RED and names all four leaked chains; each of the six in-tree
protective copies at a corpus-function argument site is individually pinned; wrapper
isolation is identity-exact with zero survivors; the negative control's disarm
substitution is a real assertion (a refactor of the blocker line turns it RED, not
vacuous); ledger append-only holds; all hard gates pass at `62d6cd9` (goldens unchanged
since `da320eb`, node list = baseline + five, `manifests --base` ok at 9795, archive 10/10,
full suite 9778 passed / 17 skipped).
