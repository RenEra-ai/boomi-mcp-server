# Audit ledger — issue #172 (M12.13 follow-up)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`.
Conventions inherited from #152's end state apply from row one. **Every platform claim in
this slice carries a provenance marker** (`measured here` / `documented, not measured` /
`assumption`) — that discipline is not decoration here, it is the reason the issue exists:
the specification has been wrong about this question twice, each time by asserting platform
behaviour instead of measuring it.

## Baseline (Stage-1 step 0)

- Issue: #172 — required-status viability and the `[skip ci]` hole are undecided pending a measured ruleset experiment
- Step-0 baseline: `ffae2a19906810e34ccec72628e29d09a7b4561e`
- Slice kind: dark (docs only, plus one comment-only workflow hunk; no `src/`, `server.py`
  or executable `scripts/` change). The slice's DELIVERABLE is a measurement and the
  documentation that records it, not code.
- Artifact trust boundary: the slice CREATES AND OWNS this ledger, its evidence archive
  (including the raw API and push captures), the rewritten §10 and README passages, and —
  temporarily — one disposable branch and one repository ruleset. It CONSUMES, unchanged:
  `.github/workflows/tests.yml`'s executable content, the check name `Python 3.11 non-KB`
  that #153/#154 cite, and every manifest row.
- Expected defect classes (pre-enumerated from #165's and #173's end states, so a second
  instance triggers structurally ON ARRIVAL): a doc/record claim about behaviour that no
  check enforces or that the tree contradicts; a hand-typed count or quotation restated
  rather than derived from the capture; **a platform claim asserted rather than measured**
  (this slice's own subject).

## Deviation (owner-instructed, recorded before the first round)

Same as #165 and #173 and on the same instruction: this setup slice runs WITHOUT Codex,
with independent fresh-context Claude reviewer agents as the Stage-2 substitute. Precedent
and its location are recorded in `ISSUE_165_AUDIT_LEDGER.md`'s Deviation section, which
quotes issue #171's body verbatim. No Codex attestation is claimed anywhere in this ledger,
and the archive holds no collector run directories.

Loop-length discipline carried forward from #165's CP-3 and #173's CP-1: non-blocking
residue gets ONE batched correction pass that never reopens a gate.

## Owner decision recorded before the experiment

Asked and answered 2026-08-16, before any ruleset was created: **if both controls pass and
the measured fast-forward to `dev` succeeds, the required-status-check ruleset on `dev`
REMAINS.** The owner accepted its consequence explicitly — every future `dev` landing must
first earn a green `Python 3.11 non-KB` check via a `scratch/**` preflight push, the route
#171 built — and that reverting is a single API call. Criterion 7's "temporary ruleset
removed" therefore covers the SCRATCH experiment rule; the `dev` rule is criterion 5's
outcome, not a temporary artifact.

## Loop roster (fixed BEFORE the first correction)

1. Stage-1 QA — darkness proof (empty `src/` + `server.py` diff; the workflow hunk is
   comment-only) PLUS the experiment itself, whose controls are the measurement this slice
   exists to produce.
2. Stage-2 substitute review — independent Claude reviewer agents, delta-scoped per round.
3. Composite wave gate — local `wave` run on the slice tip.
4. Terminal correction loop — ONLY via a recorded roster-addition checkpoint.

## Pre-experiment state (captured before anything was created)

All `measured here`, 2026-08-16, and archived under
`docs/architecture/evidence/issue-172/rulesets/`:

```
gh api repos/RenEra-ai/boomi-mcp-server/rulesets            -> []
gh api repos/RenEra-ai/boomi-mcp-server/rules/branches/dev  -> []
gh api repos/.../branches/dev --jq '{name,protected}'       -> {"name":"dev","protected":false}
```

Caller permissions: `admin: true` (`measured here`) — the authority criterion 2 requires.

The check a rule must name: **`Python 3.11 non-KB`**, app `github-actions`,
`integration_id` **15368** (`measured here`, from the check-runs API on the baseline). It
is the job's `name:` field, not the job id and not the workflow name.

**A rule must name that context explicitly; a suite-level or "all checks" rule must not be
used.** A `google-cloud-build` app creates a permanently QUEUED check suite with zero check
runs on every push to this repository (`measured here`, during #172 recon), so a rule that
waits on all suites would never be satisfiable. This is recorded because it is the kind of
platform fact the specification has previously assumed rather than measured.

## Experiment conduct (Stage-1 QA — the measurement IS this slice's deliverable)

Every line below is `measured here`, 2026-08-16; raw captures under
`docs/architecture/evidence/issue-172/rulesets/`.

| Phase | Action | Result |
| --- | --- | --- |
| 1 | capture pre-state and the green candidate `H` = `ffae2a1` | `rulesets -> []`, `rules/branches/dev -> []`, `branches/dev protected=false`, caller `admin: true`; `H` carries `Python 3.11 non-KB` from `github-actions` (15368) |
| 2 | create disposable branch at `0df53ff`, then the enforcing ruleset (id 20913144) on it | created; `enforcement: active`, `bypass_actors: []`, context pinned by name + `integration_id` |
| 3 | NEGATIVE control — push an unchecked `[skip ci]` commit | **REFUSED**: `GH013 … Required status check "Python 3.11 non-KB" is expected.`, exit 1, branch unmoved |
| 4 | POSITIVE control — advance to the exact already-green `H` from another ref | **ACCEPTED**: exit 0, `0df53ff..ffae2a1` |
| 5 | enable the same rule on `dev` (id 20913606) | active, no bypass actors; `rules/branches/dev` now reports `required_status_checks` |
| 7 | delete the experiment ruleset and branch; capture after-state | exactly one ruleset remains (the `dev` rule); zero `scratch/**` branches |

**Two conduct notes, recorded because a measurement's credibility rests on how it was
taken:**

1. The first negative-control push failed with `src refspec … does not match any` — a
   shell parameter-expansion bug (`"$NEG:refs/…"` parsed as `${NEG:offset}`) that ate the
   refspec colon, so **no push ever reached GitHub**. That was an operator error, not a
   rule refusal, and it is NOT the recorded result: the control was re-run with the
   refspec correctly quoted, and the archived transcript is that run. Recording the
   discarded attempt matters — a botched command that happens to "fail" is exactly how a
   negative control gets faked.
2. The positive control was deliberately held until **both** check runs on `H` had settled
   to `success`. While the `dev`-push run was still `in_progress`, a refusal could not
   have been attributed unambiguously to the SHA-attachment question rather than to an
   in-flight required context. The wait removes that confound.

## Defect-class ledger (classes assigned at reconciliation; counts derived from the rows)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-D-1 | inherited seed — `ISSUE_171_AUDIT_LEDGER.md` row D-1 (superseded by D-1a…D-1d), deferred at #171's CP-6/CP-7 | "Is a required status check viable now?" — #171 added a `push: scratch/**` preflight route, so a commit CAN now acquire a `Python 3.11 non-KB` check before it reaches `dev`. Whether that makes a ruleset requiring the check viable is **undecided pending measurement** — and must stay undecided until measured. §10 "has now been wrong about this question in *both* directions (first calling a preflight impossible, then calling it available-but-disallowed), each time by asserting platform behaviour instead of measuring it. A third prediction is not wanted." | **P2-equivalent** | **capability reachability** — whether the gate can be made preventive at all | *(a platform claim asserted rather than measured — the class this slice exists to close)* | Standard — anchor: no source critical label; lineage: FIRST deferral, reason class `blocked-by-mechanism` (repo-admin authority on a live branch), so it may still be deferred again if the mechanism remains blocked | this slice | `fixed` — **MEASURED, not predicted.** Both controls passed. A ruleset naming the context `Python 3.11 non-KB` + `integration_id` 15368, `enforcement: active`, no bypass actors, on a disposable branch: the NEGATIVE control (an unchecked `[skip ci]` commit) was refused with `GH013 … Required status check "Python 3.11 non-KB" is expected.`, exit 1, branch unmoved; the POSITIVE control (the exact already-green `ffae2a1`, both its check runs settled `success` first, pushed from a different ref) was accepted, exit 0, `0df53ff..ffae2a1`. A required check is therefore viable here AND honours a check attached to the SHA rather than the branch. Per the owner decision recorded above, the same rule is now active on `dev` (ruleset id 20913606); the experiment ruleset and branch are deleted, with the after-state captured. |
| INH-D-2 | inherited seed — `ISSUE_171_AUDIT_LEDGER.md` row D-2 (superseded by D-2a…D-2d), deferred at #171's CP-6/CP-7 | "The `[skip ci]` hole" — a push whose head commit message carries `[skip ci]` starts no workflow run at all, so the tip lands unchecked (§10 gap 1). "A skipped workflow cannot repair its own absence: nothing inside the pushed tree can observe a run that never started. The only mechanism that closes this is the same repository rule as item 1 — which is why the two are filed together rather than separately. Item 1's **negative control (criterion 3) is exactly this hole's test.**" | **P2-equivalent** | **capability reachability** — an unchecked tip can reach `dev` | *(same class as INH-D-1; one mechanism closes both)* | Standard — anchor: no source critical label; lineage: FIRST deferral, reason class `blocked-by-mechanism` | this slice | `fixed` — the negative control IS this hole's test, and it passed: a commit carrying `[skip ci]`, which starts no workflow run at all and therefore carries no check, was REFUSED by the rule. That is the first measurement of gap 1's premise in this repository, and it settles the gap on `dev`: such a tip can no longer land unchecked. The hole remains open on branches the rule does not cover (including `scratch/**` preflights), which §10 gap 1 now states explicitly rather than leaving implied. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement).

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

## Deferrals

*(none recorded at instantiation)*

## Evidence index

Archive root: `docs/architecture/evidence/issue-172/` with `index.jsonl` + `SHA256SUMS`
created in the same Stage-1.5 commit as this file. It holds the experiment's raw captures
under `rulesets/` — the pre-state, the created rule, and the verbatim stdout/stderr and
exit codes of both control pushes — plus reviewer artifacts under `substitute-reviews/`.
No collector run directories exist for this slice (see the Deviation section). The Actions
capture convention follows #171: `actions/` directories are evidence, deliberately NOT
indexed as `index.jsonl` run rows, because the verifier accepts only collector schemas.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
