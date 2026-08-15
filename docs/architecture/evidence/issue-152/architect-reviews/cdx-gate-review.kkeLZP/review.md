1. Critical — scripts/wave_gate.py:2294-2299 and 1574-1579: ownership is recorded without exclusive creation. `O_CREAT | O_TRUNC` lacks `O_EXCL`/`O_NOFOLLOW`. Reproduced: a foreign `collected.txt` was overwritten, then `dispose()` returned `True` and deleted it; the worktree fingerprint stayed unchanged, so the gate can pass after unrelated data loss. This is not #164’s removal race—ownership was never established. Create exclusively and record ownership only after success.

2. P1 — scripts/wave_gate.py:2128-2133: the closing `_status(repo)` fingerprint remains outside the `BaseException` normalization. Reproduced by making that second call raise `SystemExit(0)` after validation; it escaped `main()` with process status 0. Extend the unexpected-error boundary through the closing sequence.

3. P2 — docs/architecture/ENDGAME_VERIFICATION_GATE.md:606-611: the section still says the `BOOMI_*` variables are absent/dropped, contradicting the workflow and lines 622-629, which say they are set. Lines 271-280 also describe the removed `_unlink_tree_at()` cleanup. The prior documentation finding is therefore not fully closed.

The golden-child isolation, strict `bootstrap_base` typing, and README token fixes are closed, and the targeted new tests pass. The scratch, unexpected-exit, and documentation findings remain unresolved, so #164 and #165 are not the only outstanding items.

VERDICT: ISSUES FOUND
