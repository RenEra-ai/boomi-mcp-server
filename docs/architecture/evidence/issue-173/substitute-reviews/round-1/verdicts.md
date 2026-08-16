# Round-1 substitute review — charter and verdict (issue #173)

Reviewed delta: `0df53ff1367fe1bd273f5362864ed6a2aa1ab054..a996b4dc61edfc01e43bde7858ed06b0ab3b1d6f`
(the whole slice). One independent fresh-context Claude reviewer, read-only, mutation
experiments in throwaway `git worktree` checkouts. No Codex — see the Deviation section of
`docs/architecture/ISSUE_173_AUDIT_LEDGER.md`.

## Verdict

`VERDICT: ISSUES FOUND` — one P1, one P2, four P3 (ledger rows R1-1 … R1-6).

## Findings

- **P1 — the per-ledger `assert parsed` deadlocks new-ledger instantiation.** The workflow
  instantiates a ledger at Stage-1 step 0 and requires a green suite BEFORE the Stage-1.5
  commit, so a fresh ledger could never be validated and therefore never committed.
  Reproduced baseline-vs-tip. `ISSUE_152_AUDIT_LEDGER.md` has zero `INH-*` rows — a
  committed counterexample of a slice that inherits nothing, so the seeding convention
  cannot serve as the enforcement. (R1-1)
- **P2 — the slice's only deferral is invalid on three counts**: filed against #164, which
  does not cover the subject; its justification contradicted by an in-slice measurement
  (co-deletion IS caught, because it necessarily deletes the ledger); and it cited a
  checkpoint row that did not exist. (R1-2)
- P3 — the history walk lacked `-z` (git quotes non-ASCII paths, so the anchored regex
  misses them) and `--no-renames` (a rename-created ledger never enters the frozen set);
  both escapes demonstrated. (R1-3)
- P3 — the whitespace fixture did not actually pin `-z`: replacing it with `.splitlines()`
  left every test green, and the naive-split reproduction was tautological. (R1-4)
- P3 — the finding-id regex was hand-copied to a third site. (R1-5)
- P3 — two stale hand-typed measurements (#171's archive has 146 tracked paths, not 98;
  1431 commits, not 1425) and two record inconsistencies. (R1-6)

## Verified clean by this reviewer

Each item-3 fix individually pinned; item 1 pinned against the genuine historical parser
(`ln.count("|") > 8`, found at `2d37bee`), which the recipe's own mutant did not faithfully
reproduce; the symlink fixture faithful (mode 120000 at commit time, `is_symlink()` False);
history walk correct where it matters and costing 0.10 s over 1431 commits; ledger
integrity exact (only the two revision rows added, zero committed bytes changed,
supersession map 26/26, DC-16 arithmetic re-derived); manifest exact and equal to the real
collected set both ways; extraction preserved semantics; full suite 9785 passed / 17
skipped.
