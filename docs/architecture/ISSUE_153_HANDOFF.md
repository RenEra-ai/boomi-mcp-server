# Issue #153 (M12.15) — resume handoff

Authoritative resume record. Everything needed to continue is here or in
`docs/architecture/ISSUE_153_AUDIT_LEDGER.md` (the durable audit record).

## State

- Branch `codex/issue-153`, **HEAD `0003bf1`**, worktree clean, **38 commits** since baseline.
- Step-0 baseline: `9f19aad5b280d58c02ef5cd840ff150d0193c1dd` (== `origin/dev`).
- **NOT pushed. Issue #153 OPEN, and it must stay open** — see the determination below.
- Full non-KB suite: **10026 passed / 17 skipped**. Wave-gate scanners: 326 passed.
  #149 reachability freeze: green, census rebaselined, canonical route classified.

## The determination: INCOMPLETE

The §6 architect implementation review (L3) ran for the first time at `1df67f1` and returned
`ISSUES FOUND` with **ten findings, five Critical-tier** — recorded as rows `AR1-01` … `AR1-10`.

`CLAUDE.md`'s bar is zero unresolved critical findings. There are five, and none may be deferred:
three sit in mutation accounting, one in capability reachability. **So the slice terminates
INCOMPLETE and the issue stays open.**

Why this gate found what twelve clean-trending commit-review rounds did not: it asks a different
question — *does the implementation build what the PLAN specified?* Four of the ten are plan items
never implemented at all. No code-quality review surfaces those, because the code that exists is
correct; it is the code that does not exist that is the finding.

## What IS established and does not need redoing

| Gate | Status |
|---|---|
| §3 Codex architect plan | PASSED, attested (`.codex/plans/issue-153.md` + `.attest.json`) |
| L1 Stage-1 live QA | **CLOSED CLEAN at round 11** — zero findings, nothing owed. 12 findings across r1–r10, all fixed. Verified the headline capability unaided at the public boundary. |
| L2 Stage-2 Codex commit-review | **CLOSED CLEAN at rounds 9 and 12.** 28 findings across 12 rounds, all fixed. |
| L3 §6 architect review | **ISSUES FOUND — 10 open.** Archived at `evidence/issue-153/architect-reviews/cdx-gate-review.tD0IwO`. |
| L4 composite wave gate | **NOT re-run at the final SHA** (last passed at `acdb793`, long superseded). |

The capability itself works and is live-verified: `plan → compile → apply` creates a real process
component; readback carries zero `id-<key>` placeholders and real Boomi ids; multi-root late
binding emits the child's real id in `<processcall processId=…>`; both attestations are served and
recorded; update-preservation digests the MERGED bytes; `conflict_policy="clone"` creates a
suffixed component and leaves the original byte-identical.

## Next actions, in order

1. **Fix the five Critical `AR1-*` rows.** Suggested order by tractability:
   - **AR1-02** (relocatability misses the IR) — smallest. `iter_component_refs` in
     `models/process_ir.py` already enumerates every `ComponentRefV1` in an IR, so extend
     `envelope_relocatability_offenders` to walk the IR too. The reviewer built the violating case.
   - **AR1-05** (attestation inputs) — three sub-items; (a) execution passes
     `config.get("account_id")` instead of the account typed preflight derived, which lets a caller
     influence the attested scope hash.
   - **AR1-01** (compiled-plan identity) — assert the apply-built plan's fingerprint equals the
     compile artifact digest, with the recorded clone divergence as the sole exception.
   - **AR1-04** (durable partial evidence) — pre-mutation `in_progress` build record and a
     `failed_partial` transition, so a lost response does not lose the record of a write.
   - **AR1-06** (folder placement) — a whole plan item; `PROCESS_MATERIALIZATION_PLACEMENT_*` are
     registered and unreachable.
2. **AR1-09** is the highest-value Standard: served guidance in `meta_tools.py` and `server.py`
   still says direct ProcessIR is never applied and ends at compile, contradicting the shipped
   capability. Served contract text is a blocking class.
3. Each correction gets affected QA + a delta-scoped Codex review, then **re-run §6**.
4. **Re-run the wave gate at the FINAL sha, from a verified-clean tree.**
5. Ledger final-tree validation table, then finish = FF-push to `dev` + close, **NO PR**.

## Hard-won gotchas (do not rediscover)

- **While ANY gate is in flight, the tree is frozen.** I broke this four times; each time a Codex
  round landed mid-QA and I edited. The rule that actually holds is **serialize the gates** — never
  dispatch QA while a review is running or while any finding is unapplied. QA's
  `.claude/agent-memory/boomi-qa-tester/harness/tree-freeze-guard.py` hashes the bytes of every
  loaded module and exits 3 on a violation; run every scenario inside it.
- **Never edit the tree while `wave_gate.py` runs** — it diffs `git status` and returns
  `WORKTREE_DIRTY`.
- **A guard that re-states the rule instead of exercising it protects nothing.** Six instances this
  slice. Drive the production entry point; mutation-control in both directions.
- **A "derived" fix is only as good as the authority chosen.** `retryable` was the wrong authority
  for "did we write" — retryability is about the request, not the write. Ask the component that
  performs the action.
- Ledger prose must not contain diagnostic-shaped tokens unless allowlisted in
  `tests/test_wave_gate.py::_LEDGER_NON_DIAGNOSTIC_TOKENS`; finding rows must form ONE contiguous
  table (no blank line between rows).
- A gate-review archive needs `prompts/` and an index row whose `status` comes from
  `attestation.json`'s `turn.status`, not a top-level `status`.
- Editing a `RECIPE_LAYER_MODULES` member moves served digests → rebaseline
  `tests/_m12_12_legacy_inventory.py --write <fixture>` AND regenerate the §11 tables in
  `M12_COMPATIBILITY_INVENTORY.md`, anchored on their subsection headings (three share a header row).
- Suite: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 BOOMI_LOCAL=true .venv/bin/python -m pytest tests --ignore=tests/kb -q`
  (~13 min).
- renera account expires **2026-08-28**.
