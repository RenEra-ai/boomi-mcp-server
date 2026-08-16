# Round-3 substitute review — charter and verdict (issue #165)

Reviewed delta: `0b91c80f7c9af023a9b88a930d0365dd36edfe9a..06976a0d9ef856edaa2b661cceef9ab315f47308`
(the round-2 correction only). One independent fresh-context Claude reviewer, read-only,
with mutation experiments in throwaway `git worktree` checkouts. No Codex; see the
Deviation section of `docs/architecture/ISSUE_165_AUDIT_LEDGER.md`.

## Charter

Verify each round-2 fix claim by experiment; attack the new antecedent test specifically
(interception completeness, restore leakage, structural blind spots, depth limits,
vacuity guards); verify ledger append-only integrity, the recomputed defect-class counts,
and CP-1's factual claims; re-run the hard gates.

## Verdict

`VERDICT: ISSUES FOUND` — two P2 and four P3.

## Findings (ledger rows R3-1 … R3-6)

- **P2 — the antecedent guard does not do what it says; a real protective copy can be
  deleted with nothing objecting.** The watch set held only TOP-LEVEL module attributes
  and the leak walk stopped at depth 6, while real recorded arguments nest to depth 9;
  private helpers (including the actual hop into production) were unwrapped. Reverting the
  in-tree `copy.deepcopy(LISTENER_CHAINS[chain])` left all 69 tests green while production
  genuinely received the live module objects. The same overclaim was published in the
  corpus CONTRACT and in the served spec. (R3-1)
- **P2 — a defect class taken to four instances with no structural fix or recorded
  deviation**, justified by a CP-1 claim its own rows contradict ("all of them the SAME
  guard" — in fact two distinct guards). (R3-2)
- P3 — R2-7 recorded `fixed` but closed only one of three orphaning blank lines; every row
  the round-2 commit appended was still outside the table. (R3-3)
- P3 — CP-1's breadth enumeration is contradicted by its own rows. (R3-4)
- P3 — the ledger's own archiving rule was unmet for round 2 (no `round-2/` artifact). (R3-5)
- P3 — the rebind claim is stronger than the equality comparison implements. (R3-6)

## Verified clean by this reviewer

R2-1's specific fix is real: the three notify-site copies are genuinely pinned (reverting
them reports exactly five leak paths). Wrapping mechanics sound; `finally` restore
complete and identity-exact. R2-2, R2-3, R2-5, R2-8, R2-9 correct and complete. Ledger
append-only holds — no finding row's bytes changed; the three changed rows are `DC-*`
aggregates, which the append-only parser excludes by design. Supersession map matches the
revision rows; defect-class counts matched the rows; CP-1's "zero critical at every round"
correct under CLAUDE.md's anchors. Hard gates all pass at `06976a0`: goldens unchanged
since `da320eb`, node list = baseline + three, `manifests --base` ok (9793), archive
checksums 7/7, full non-KB suite 9776 passed / 17 skipped.
