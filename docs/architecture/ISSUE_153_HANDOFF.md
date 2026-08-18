# Issue #153 (M12.15) — resume handoff

Written before a context compaction. Everything needed to continue is here or in
`docs/architecture/ISSUE_153_AUDIT_LEDGER.md` (the durable audit record).

## State

- Branch `codex/issue-153`, **HEAD `d94273e`**, worktree clean, **17 commits** since baseline.
- Step-0 baseline: `9f19aad5b280d58c02ef5cd840ff150d0193c1dd` (== `origin/dev`).
- **NOT pushed. Issue #153 still OPEN.** Do not push or close until the gates below pass.
- Driver: `/codex-issue 153`, main-thread mode (no `.claude/workflows`), so `CLAUDE.md`'s
  own workflow is the lifecycle.

## Implementation: all 10 planned steps landed

Six new modules + wiring:
- `models/process_component.py` — `ProcessComponentEnvelopeV1`, `ProcessAuthoringUnitV1`
- `authoring/process_materialization.py` — relocatable plan + fingerprint
- `compiler/process_ir/execution_profile.py` — compiler-derived scheduled/listener
- `categories/components/process_component_materializer.py` — neutral envelope writer
- `categories/components/canonical_process_apply.py` — late binding + 2 attestations
- `categories/components/builders/_process_preservation.py` — ONE shared policy

Wire change (breaking, no alias): `process_ir` intent now carries `units: [{envelope, process_ir}]`;
`AUTHORING_CONTRACT_VERSION = "2"`; `authoring.typed_apply.process_materialization` = `supported`.

## Gates

| Gate | Status |
|---|---|
| §3 Codex architect plan | **PASSED, attested** (`.codex/plans/issue-153.md` + `.attest.json`) |
| Composite wave gate (L4) | **PASSED at `acdb793`** — 9980 passed, 60 goldens byte-exact, `plan fingerprint checked:2 case(s)`, EXIT=0. Evidence archived at `docs/architecture/evidence/issue-153/wave-gate/`. **Must be RE-RUN at final SHA.** |
| Stage-1 live QA (L1) | r1 found 4 findings (2 Critical) — **all fixed in `d94273e`**. **r2 fix-delta was RUNNING at compaction time** (agent may have died with the session — verify, re-dispatch if no r2 report). |
| Stage-2 repo Codex review (L2) | **NOT RUN** |
| §6 architect implementation review (L3) | **NOT RUN** |

## QA round 1 findings — all fixed, all need r2 live re-verification

Report: `agents/reports/2026-08-17-issue-153-m12-15-stage1-r1.md` (`agents/` is gitignored).
`INDEX.md` never got r1's line — the r1 agent died at "Now let me write the report" (auth expiry).

1. **QA-153-r1-01 (Critical)** — typed apply raised `KeyError: 'existing_component_id'` for every
   spec with process units; capability unreachable. Fixed: canonical step carries
   `existing_component_id`/`planned_action`, and `_apply_plan` gained `_execute_canonical_process`
   BEFORE the `components_by_key[key]` lookup.
2. **QA-153-r1-02 (Critical, secrets)** — the S3-01 "fix" never took effect: the legacy
   component-plan echo overwrote the withheld preview ~50 lines later. Fixed via
   `_withhold_process_roots`, re-applied AFTER the echo. Regression test
   `test_the_legacy_component_plan_echo_cannot_restore_withheld_roots` FORCES the echo
   (verified: fails pre-fix, passes post-fix).
3. **QA-153-r1-03 (High)** — `canonical_process_apply.py` imported by NOTHING in `src/`; the
   relocatability validator was unreachable. Fixed by the same wiring as r1-01.
4. **QA-153-r1-04 (High)** — duplicate-key served as raw `ValidationError`. Fixed:
   `_named_error_code_from_validation` maps pydantic `type` -> served `error_code`.
   Measured: plan and compile now serve `INTEGRATION_COMPONENT_KEY_DUPLICATE`.

## Next actions, in order

1. Check for `agents/reports/2026-08-17-issue-153-m12-15-stage1-r2.md`. If absent, re-dispatch
   `boomi-qa-tester` for the r2 fix-delta (prompt shape: operational header + the four findings +
   the live scenarios that were BLOCKED in r1: create / readback-no-placeholders /
   update-preservation / verify / multi-root / both attestations / cleanup).
2. Fix anything r2 raises; re-run affected QA.
3. **Stage 1.5**: the QA-validated tree is already committed — record it in the ledger.
4. **Stage-2 Codex commit-review** per `CLAUDE.md` §5b–5e (detached, polled across Bash calls,
   ALWAYS collected via `commit-review-collect.mjs`; never read findings from `wait`).
   `--base 9f19aad5b280d58c02ef5cd840ff150d0193c1dd` for round 1.
5. **§6 architect review** — gate-bound session, `--gate review`, plan `cat`'d in VERBATIM.
6. Re-run the wave gate at the FINAL sha from a verified-clean tree.
7. Ledger: final-tree validation table + close. Then `/codex-issue` finish = FF-push to `dev`
   + close issue, NO PR (per repo convention).

## Hard-won gotchas (do not rediscover)

- **Never edit the tree while `wave_gate.py` runs** — it diffs `git status` before/after and
  returns `WORKTREE_DIRTY`. Happened TWICE. Launch only from a verified-clean tree.
- **A guard that doesn't exercise the breaking path passes for the wrong reason.** Two instances
  this slice: the preservation witness (refuted by adversarial review) and my first r1-02 probe
  (the legacy echo never runs without a live client, so the mutant "passed" too).
- The evidence archive must be **git-tracked** and covered by `SHA256SUMS`, or
  `test_audit_ledger_attestations_have_durable_matching_evidence` fails.
- Ledger prose must not contain diagnostic-shaped tokens unless allowlisted in
  `tests/test_wave_gate.py::_LEDGER_NON_DIAGNOSTIC_TOKENS`.
- Renaming a required test node needs a **tombstone** in `tests/fixtures/wave_gate/test_nodes.jsonl`.
- Editing a `RECIPE_LAYER_MODULES` member moves served digests -> rebaseline
  `tests/_m12_12_legacy_inventory.py --write` AND regenerate the §11 markdown tables in
  `docs/architecture/M12_COMPATIBILITY_INVENTORY.md` (two-way check).
- Suite: `PYTHONPATH=src PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 BOOMI_LOCAL=true .venv/bin/python -m pytest tests --ignore=tests/kb -q`
  (~13 min, no xdist). Emitter symbol requirement wants `connector-action`, NOT
  `connector-operation`.
- renera account expires **2026-08-28**.
