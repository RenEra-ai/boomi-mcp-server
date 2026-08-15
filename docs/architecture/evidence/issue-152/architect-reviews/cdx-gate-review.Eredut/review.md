I’ll review the actual `1779e5b..HEAD` delta and current tree, with separate checks for the round‑9 fixes, bootstrap/transition semantics, and fail-open paths. I’ll keep this read-only and only report reproducible, actionable defects.The current worktree is clean at `1083413d`. I’ve confirmed the landing delta starts from `9080e3c`, where both manifest paths are absent; I’m now checking that the committed bootstrap itself satisfies the one-time schema and ordering rules, not treating intermediate branch commits as ledger transitions.The focused gate tests are green (207/207), but two fail-closed edge cases have reproduced outside that matrix. I’m validating whether they are genuinely distinct from the explicitly excluded #164 race before including them.I agree that the landing event is a valid bootstrap, but the implementation still has actionable issues.

### Findings

1. **[Critical] Scratch cleanup can delete unrelated data and still pass.**  
   [`_ScratchDir._dispose()`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:2302) recursively deletes every entry through [`_unlink_tree_at()`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:2454), without proving those entries were created by the gate. I moved an unrelated directory containing `precious.txt` into a valid scratch before disposal; `dispose()` returned `True`, the file was deleted, and the worktree fingerprint remained unchanged. This is not #164: the scratch binding never changes, no post-`rmdir` observations are interleaved, and the lost object is a nonempty subtree rather than an empty untracked directory. Cleanup must inventory/verify owned entries and refuse unknown replacements instead of recursively deleting everything present.

2. **[P1] An unexpected `SystemExit(0)` still makes the gate green.**  
   [`execute()`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:2064) records unexpected `BaseException`s, performs cleanup, then [re-raises the original exception](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:2110). Since [`main()` only catches `GateFailure`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:2754), `SystemExit(0)` escapes as process status 0. Reproduced through the real `main()` by making `collect_nodes()` raise `SystemExit(0)`; the command exited 0 after manifest validation. Normalize unexpected exceptions to a status-1 `GateFailure`. Also move [`make_scratch_dir()`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:2035) inside the closing-fingerprint boundary.

3. **[P1] The two golden passes do not have isolated runtime filesystems.**  
   Although separate request directories are created at [`_render_pass()`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:1555), both children still run with the repository as `cwd` and the same inherited `TMPDIR` at [the subprocess invocation](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:1573). A synthetic renderer whose seed-2 output differed but cached through `tempfile.gettempdir()` made both passes return seed-1 bytes, and `check_goldens()` accepted them. Set each child’s `cwd` and `TMPDIR` to its pass directory. Thus round-9 finding 5 is not effectively closed.

4. **[P2] `bootstrap_base` still accepts the wrong JSON type.**  
   [`parse_manifest()`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/scripts/wave_gate.py:640) applies the SHA regex to `str(value)`. A JSON integer containing 40 decimal digits therefore parses successfully; identical malformed base/current manifests also pass transition validation. Require `type(bootstrap_base) is str` before matching.

5. **[P2] The durable documentation contradicts the landed contract.**

   - [Golden assignment](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ENDGAME_VERIFICATION_GATE.md:165) still says 58 survivors and no `deletion_only`, contradicting the immutable `#160/deletion_only` row.
   - [Transition rules](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ENDGAME_VERIFICATION_GATE.md:351) still permit born-tombstoned rows, which the gate now rejects.
   - [CI environment](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/ENDGAME_VERIFICATION_GATE.md:589) says the three `BOOMI_*` variables must remain absent, although the workflow now requires them.
   - [README](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/README.md:562) documents the wrong fingerprint token; the contract is `PLAN_FINGERPRINT_PENDING issue=#153`.

### Bootstrap judgment

The consequential reasoning is correct:

- `dev` and `origin/dev` are `9080e3c`; neither manifest exists or was touched in that commit’s full ancestry.
- The landing push therefore takes the bootstrap path. Intermediate feature-branch commits are not ledger transitions.
- The test ledger has 9,752 all-active, sorted, sequential rows, matches current Python 3.12 collection, and contains all 9,478 baseline nodes.
- The golden ledger has 60 all-active, sorted, sequential rows: 57 survivors, two `#159/transitional_oracle`, and the correct `#160/deletion_only` row.

Behaviorally, findings 2, 4, and 6–13 are closed. Findings 1, 3, and 5 retain the issues above. I accept the stated disposition of finding 14 for the authoritative CI/push path.

Focused validation passed: 207 wave-gate tests and 66 golden tests.

VERDICT: ISSUES FOUND
