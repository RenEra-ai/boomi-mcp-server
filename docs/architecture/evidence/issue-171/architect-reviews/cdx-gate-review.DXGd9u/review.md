I’m reviewing only `2682dd972…d6d3e507`, with separate checks for archive closure, the new manifest test, and ledger/checkpoint consistency. I’ll verify claims against Git, run the focused tests, and report only actionable regressions.The delta is cleanly bounded and the worktree is clean. One wording pattern is already under scrutiny: the new CP-2 note itself says “final replay,” so I’m checking whether that is merely chronology or another premature-state claim before deciding severity.The focused pytest launcher has a stale shebang pointing at the repository’s former path. I’m bypassing only that wrapper with the environment’s Python module entry point; this does not change the tree. Separately, the diff confirms several “append-only” fixes rewrote committed rows in place, which is likely blocking this replay.The executable checks are green (`245` gate tests, build-context check, manifest transition, archive hashes), but the review is not clean: the correction repeats the append-only defect in four committed rows, and the claimed Git-index archive invariant is still only a one-off measurement. I’m reconciling these with the closure order now.Issues found.

1. [ISSUE_171_AUDIT_LEDGER.md:179](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:179)–181 and [line 186](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:186) rewrite four rows committed before this delta: `INH-TC1-2a`, `R5-1a`, `R5-2a`, and `AR2-4`. That directly repeats DC-18 while fixing AR3-4/AR3-6. Restore their `2682dd9` text, append superseding rows (`AR2-4a` and new `b` revisions), and declare every mapping.

2. [ISSUE_171_AUDIT_LEDGER.md:75](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:75) claims the archive is now verified against `git ls-files`, but the scanner at [tests/test_wave_gate.py:4848](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_wave_gate.py:4848) still compares `SHA256SUMS` only with `Path.rglob()`—the working tree. An ignored, untracked file that exists locally and is listed in `SHA256SUMS` still passes locally and fails after checkout, reproducing AR3-1. Add a Git-index-derived set comparison.

3. The new append-only test is only partly sound:

   - [tests/test_wave_gate.py:1767](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_wave_gate.py:1767) requires more than eight pipe delimiters, excluding all #152 ledger tables; `checked` remains satisfied by #171.
   - [line 1794](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_wave_gate.py:1794) searches the entire document for mapping text. Removing a mapping from the supersession-map section but quoting it elsewhere still passes.

   Parse the bounded map and compare it exactly with detected revisions; add in-memory negative fixtures. The disclosed lack of cross-commit byte checking is acceptable only because review remains responsible for it—and this replay found the resulting violations.

4. [ISSUE_171_AUDIT_LEDGER.md:532](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:532) calls AR3-2’s note “final replay.” It came from replay #3, while replay #4 is this review. This repeats AR3-6’s premature provenance defect.

5. [ISSUE_171_AUDIT_LEDGER.md:193](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:193) assigns AR3-4 to DC-16, whose definition at [line 72](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ISSUE_171_AUDIT_LEDGER.md:72) is a stale derived aggregate. AR3-4 is a semantically wrong per-row class assignment, a different mechanism. Record a fitting class or revise the definition through the append-only mechanism.

Verified clean:

- `SHA256SUMS` currently matches exactly 85 tracked archive files; all hashes pass.
- Both raw logs are tracked and included in the gcloud upload; ordinary stray `*.log` paths remain ignored.
- CP-2 current-state prose says it authorizes nothing and D-1a/D-2a are void pending CP-3.
- The current ten revision rows are explicitly mapped.
- Manifest transition is exactly one append, zero tombstones, with floors `9789 → 9790`.
- `tests/test_wave_gate.py`: 245 passed; focused build-context and append-only checks pass.
- The printed class arithmetic currently reproduces DC-12 = 6 and DC-20 = 3, but only after accepting the invalid in-place rewrite.

CP-3 remains blocked. After these fixes, run affected QA, the fix-only repo review, and architect replay #5. After CP-3 and the later administrative commits, the final tree also needs architect-gate coverage—not only the repo review—before the exact-SHA GREEN run. After the fast-forward, the planned scratch-ref/worktree cleanup, prune, and seed-SHA reachability proof still remain.

VERDICT: ISSUES FOUND
