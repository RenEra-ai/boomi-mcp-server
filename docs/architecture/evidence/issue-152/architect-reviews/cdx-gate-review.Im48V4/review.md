I’ll review only `00146c5..HEAD`, with special attention to whether removing the local landing heuristic is structurally sound and whether any corrected path can still fail open. I’ll inspect the implementation and tests in parallel, then report only actionable findings introduced by this delta.The central removal is defensible as a trust-boundary change: a local operator already controls `--base`, while CI remains the authority that binds the baseline. I’m now probing the strengthened worktree fingerprint and ambiguity path for edge cases where the implementation’s “content fingerprint” claim may still be weaker than the actual bytes.The local-bootstrap removal is a legitimate structural resolution. A local operator already controls `--base`; local ref topology cannot authoritatively prove “landed.” Keeping that authority in push/PR CI, with an explicit local assertion and warning, is the sound boundary.

Findings:

1. Critical — [wave_gate.py:307](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:307) stores complete staged and unstaged patches, then [wave_gate.py:1815](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:1815) prints them verbatim on `WORKTREE_DIRTY`. I reproduced an uncommitted `TOKEN=SUPER_SECRET_VALUE` appearing in the diagnostic. Hash patch bytes and log only safe digests/status.

2. Critical — the worktree fingerprint still fails open for some content:

   - [wave_gate.py:333](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:333) represents unreadable files with a stable errno token. Chmod/write/chmod of an existing unreadable file produced identical snapshots.
   - Default `git diff` abbreviates binary blob IDs. Two different binary contents sharing the seven-character prefix produced identical snapshots in a default-configured repo.

   Use raw `--binary --full-index` output or direct content hashing, and refuse validation when any enumerated file cannot be hashed.

3. Standard — [wave_gate.py:387](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:387) still does not prove a PR merge checkout’s tree. I constructed a commit with exactly `{head,target}` as parents but the target’s tree, omitting all head changes; it was accepted. Bind HEAD to the event’s merge commit SHA or validate the merge tree.

4. Standard — [wave_gate.py:1583](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:1583) re-raises provider-originated `GateFailure`. Because its status is unconstrained at [wave_gate.py:149](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:149), `GateFailure(..., 0)` still exits green through [wave_gate.py:1869](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:1869). Provider exceptions must always become status-1 `PLAN_FINGERPRINT_MISMATCH`.

5. Standard — the exact requested range is not a legal manifest transition. [test_nodes.jsonl:9720](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/fixtures/wave_gate/test_nodes.jsonl:9720) and [test_nodes.jsonl:9724](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/fixtures/wave_gate/test_nodes.jsonl:9724) are absent at `00146c5` but tombstoned at HEAD. Running `manifests --base 00146c5` exits 2 because [wave_gate.py:676](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:676) forbids born-tombstoned rows. Individual commit transitions pass, but a four-commit push range does not.

Non-blocking: [ENDGAME_VERIFICATION_GATE.md:269](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ENDGAME_VERIFICATION_GATE.md:269), line 316, and line 441 retain contradictory pre-correction claims.

The targeted suite passed 170/170 with plugin autoload disabled; the findings above were independently reproduced.

VERDICT: ISSUES FOUND
