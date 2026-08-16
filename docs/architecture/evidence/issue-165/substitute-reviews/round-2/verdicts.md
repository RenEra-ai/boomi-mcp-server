# Round-2 substitute review — charter and verdict (issue #165)

Reviewed delta: `7f20ea8d9fe99c226f874ddad6af552cc519f768..0b91c80f7c9af023a9b88a930d0365dd36edfe9a`
(the round-1 correction only — the underlying refactor was reviewed in round 1 and was
explicitly out of scope). One independent fresh-context Claude reviewer, read-only, with
mutation experiments run in throwaway `git worktree` checkouts. No Codex; see the
Deviation section of `docs/architecture/ISSUE_165_AUDIT_LEDGER.md`.

## Charter

Verify each of the ten round-1 fix claims by experiment where possible, and hunt for
defects the correction introduced. Named targets: the repaired import-blocker witness,
the new `modules_loaded_from_tests` assertion, the new drift invariant, the three
`copy.deepcopy` additions, the totalized recipe-arm dispatch, the three constants moved
back, the dead-code removal, ledger append-only integrity, and the hard gates.

## Verdict

`VERDICT: ISSUES FOUND` — one P2 and eight P3.

## Findings (ledger rows R2-1 … R2-9)

- **P2 — the new "executable invariant" does not pin the R1-7 fix.** Reverting all three
  `copy.deepcopy` additions leaves the invariant and all 526 tests in the affected modules
  GREEN, because today's builders do not mutate their input. The test measures the
  consequence, not the antecedent the CONTRACT bullet asserts, so the copies (and the
  bullet) remained unpinned and the ledger's claim about them was untrue. (R2-1)
- P3 — the invariant misses a REBIND (compares snapshot-time objects). (R2-2)
- P3 — "every module-level container" overclaims; the registry dict was excluded. (R2-3)
- P3 — R1-5's own count is contradicted by the tree: 14 constants across three modules,
  not 11 across four. (R2-4)
- P3 — the witness's "not armed" branch is unreachable and its diagnostic wrong. (R2-5)
- P3 — two shallow copies sit under a "deep-copies" bullet. (R2-6)
- P3 — the eleven new ledger rows sit outside the Markdown table. (R2-7)
- P3 — `import pytest` still dead in the block R1-5 pruned. (R2-8)
- P3 — typo "the corpus corpus's". (R2-9)

## Verified clean by this reviewer

All hard gates re-run and passing at `0b91c80`: 60 goldens byte-identical to `da320eb`
(full base64 payloads compared, not only hashes); node list = baseline + two nodes;
`manifests --base` exit 0; full non-KB suite 9775 passed / 17 skipped; darkness intact
(`scripts/` hunk docstring-only); archive checksums 7/7; ledger append-only holds
(INH-RD-1 byte-identical to its committed form; supersession map matches the revision
rows; no `cdx-*` citations). Eight of the ten claimed fixes fully verified, several by
mutation (blocker→`pass` FAILS; wrong meta_path ordering FAILS; broken `find_spec`
signature FAILS; both recipe-arm spellings now load-bearing; the `_linear_with_map()`
anchor is real; the file-path leak scan catches a `spec_from_file_location` smuggle).
