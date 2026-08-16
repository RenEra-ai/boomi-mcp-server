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
| 5b | **measure a real fast-forward push of an exact green SHA to `dev` under the rule** | **DONE — `measured here`.** This slice's own landing: `290b646` earned a green `Python 3.11 non-KB` check on `scratch/issue-172-preflight`, then `git push origin 290b646:refs/heads/dev` → exit 0, `ffae2a1..290b646` (transcript `criterion5-dev-fastforward.txt`). The candidate carried EXACTLY ONE check for the required context, sourced only from the scratch preflight — the isolation the positive control lacked. |
| 7 | delete the experiment ruleset and branch; capture after-state; cancel the orphaned run the positive control started | exactly one ruleset remains (the `dev` rule); zero `scratch/**` branches; `branches/dev protected=true`; run 31963168870 cancelled |

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
| **DC-172-1** | a doc/record claim about platform behaviour that the tree or the measurement contradicts — the slice's own pre-enumerated class | the captures, and the live API state | **10** — R1-1 … R1-8, T1-1, T1-2 | **Structural response, applied at the second instance (R1-2):** every `dev`-scoped statement in the served text is now split explicitly into what was MEASURED (on the experiment branch, with an archived capture) and what is INFERRED (that the identical rule behaves identically on `dev`), with the one remaining `dev` observation — criterion 5's fast-forward — recorded as OWED in the phase table rather than written in the past tense. Sibling sweep: §10 banner, §10 gaps 1–3, the artifact table, the measured-configuration block, README, and the workflow header were all re-read against the captures, and four passages contradicting the outcome were corrected. The inherited seed rows are the findings this slice discharges and are not counted as instances. |

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-D-1 | inherited seed — `ISSUE_171_AUDIT_LEDGER.md` row D-1 (superseded by D-1a…D-1d), deferred at #171's CP-6/CP-7 | "Is a required status check viable now?" — #171 added a `push: scratch/**` preflight route, so a commit CAN now acquire a `Python 3.11 non-KB` check before it reaches `dev`. Whether that makes a ruleset requiring the check viable is **undecided pending measurement** — and must stay undecided until measured. §10 "has now been wrong about this question in *both* directions (first calling a preflight impossible, then calling it available-but-disallowed), each time by asserting platform behaviour instead of measuring it. A third prediction is not wanted." | **P2-equivalent** | **capability reachability** — whether the gate can be made preventive at all | *(a platform claim asserted rather than measured — the class this slice exists to close)* | Standard — anchor: no source critical label; lineage: FIRST deferral, reason class `blocked-by-mechanism` (repo-admin authority on a live branch), so it may still be deferred again if the mechanism remains blocked | this slice | `fixed` — **MEASURED, not predicted.** Both controls passed. A ruleset naming the context `Python 3.11 non-KB` + `integration_id` 15368, `enforcement: active`, no bypass actors, on a disposable branch: the NEGATIVE control (an unchecked `[skip ci]` commit) was refused with `GH013 … Required status check "Python 3.11 non-KB" is expected.`, exit 1, branch unmoved; the POSITIVE control (the exact already-green `ffae2a1`, both its check runs settled `success` first, pushed from a different ref) was accepted, exit 0, `0df53ff..ffae2a1`. A required check is therefore viable here AND honours a check attached to the SHA rather than the branch. Per the owner decision recorded above, the same rule is now active on `dev` (ruleset id 20913606); the experiment ruleset and branch are deleted, with the after-state captured. |
| INH-D-2 | inherited seed — `ISSUE_171_AUDIT_LEDGER.md` row D-2 (superseded by D-2a…D-2d), deferred at #171's CP-6/CP-7 | "The `[skip ci]` hole" — a push whose head commit message carries `[skip ci]` starts no workflow run at all, so the tip lands unchecked (§10 gap 1). "A skipped workflow cannot repair its own absence: nothing inside the pushed tree can observe a run that never started. The only mechanism that closes this is the same repository rule as item 1 — which is why the two are filed together rather than separately. Item 1's **negative control (criterion 3) is exactly this hole's test.**" | **P2-equivalent** | **capability reachability** — an unchecked tip can reach `dev` | *(same class as INH-D-1; one mechanism closes both)* | Standard — anchor: no source critical label; lineage: FIRST deferral, reason class `blocked-by-mechanism` | this slice | `fixed` — the negative control IS this hole's test, and it passed: a commit carrying `[skip ci]`, which starts no workflow run at all and therefore carries no check, was REFUSED by the rule. That is the first measurement of gap 1's premise in this repository, and it settles the gap on `dev`: such a tip can no longer land unchecked. The hole remains open on branches the rule does not cover (including `scratch/**` preflights), which §10 gap 1 now states explicitly rather than leaving implied. |
| INH-D-1a | revision of INH-D-1 (round-1 correction, per R1-2) | corrects INH-D-1's disposition, which claimed the question settled on `dev` while no push to `dev` had yet been evaluated by the rule; INH-D-1 is retained above unedited | *(inherits INH-D-1)* | *(inherits)* | *(inherits)* | *(inherits)* | this correction | *(inherits INH-D-1's `fixed` disposition for the VIABILITY question, which the two controls do settle)* — with this correction to its scope: what is measured is that an enforcing rule with this pinned context refuses an unchecked tip and accepts a check attached to the SHA, on a branch equivalent to `dev`, and that the same rule is live on `dev` (verified via `rules/branches/dev`). What is NOT yet measured is a fast-forward to `dev` under it — criterion 5, recorded as OWED in the phase table, and discharged by this slice's own landing. |
| R1-1 | Stage-2 substitute review round 1 (independent Claude reviewer, delta `ffae2a1..6847729`) | "`.github/workflows/tests.yml` and §10: 'No ruleset depends on it … renaming it breaks those citations' — both are falsified by this very commit, and the workflow comment points at a header that now says the opposite" | **P1** | **capability reachability** — a stale authority that invites the one edit which deadlocks every landing | **DC-172-1** (a doc/record claim about behaviour that the tree contradicts) instance 1 | **Critical** — anchor: source label P1. Fixed and validated, never deferred. | `6847729` → this correction | `fixed` — both sites now state that the job name is the string the `dev` rule requires, that renaming it makes the required check unobtainable until an admin edits the rule, and that the job and the ruleset's `context` must be renamed in the same change or not at all. Correctly caught: the slice made the claim false and left it standing. |
| R1-2 | Stage-2 substitute review round 1 | "criterion 5 is half-unmet and the record silently omits it" — `dev` is still at `ffae2a1`, whose only dev-push run started ~38 min BEFORE the ruleset was created, so no push to `dev` has ever occurred under the rule, yet §10, README, the banner and the workflow header all state `dev` behaviour in the settled past tense | **P1** | **capability reachability** — an acceptance criterion recorded as met that is not | **DC-172-1** instance 2 | **Critical** — anchor: source label P1. Fixed and validated, never deferred. | `6847729` → this correction | `fixed` — REPRODUCED independently (ruleset `created_at` 17:58:06Z vs the dev run's 17:20:31Z; `dev` unmoved). The phase table now carries row 5b marking the measurement OWED; §10 gained a paragraph stating exactly what is measured on `dev` and what is inferred; and INH-D-1's scope is corrected by the revision row above. This is the slice's own pre-enumerated class — "a platform claim asserted rather than measured" — arriving in the slice that exists to end it, which is why it took the structural response recorded on DC-172-1. |
| R1-3 | Stage-2 substitute review round 1 | "surviving 'with no required check and no ruleset, nothing forces a candidate through the preflight / nothing outside the pushed tree gets a vote', 8 lines below 'PREVENTION on `dev`'" — a reader can cite either sentence | **P2** | *(machine-served contract text — the spec is the served description of the gate)* | **DC-172-1** instance 3 | Standard — anchor: source label P2 | `6847729` → this correction | `fixed` — four passages swept: the artifact table, the two "no required check and no ruleset" sentences (now scoped to "everywhere EXCEPT `dev`"), and gap 2, which is narrowed rather than deleted because its preflight-green/dev-red divergence survives. |
| R1-4 | Stage-2 substitute review round 1 | "a claim stated as measured that was not measured: 'the first measurement of gap 1's premise' — the negative-control commit was refused at the push, so no run could have started for it regardless of its message" | **P2** | *(machine-served contract text)* | **DC-172-1** instance 4 | Standard — anchor: source label P2 | `6847729` → this correction | `fixed` — the paragraph now says precisely what was measured (a tip carrying no successful required check is refused), states that it does NOT re-measure the skip behaviour, and keeps the consequence for gap 1 as a consequence rather than a measurement. The reviewer is right that §10's own provenance parenthetical already said this correctly two paragraphs earlier. |
| R1-5 | Stage-2 substitute review round 1 | "the positive control is confounded and cannot attribute the acceptance to the preflight" — at push time `ffae2a1` carried two green runs of the required context, one from the scratch preflight and one from its own `dev` push | **P2** | *(machine-served contract text)* | **DC-172-1** instance 5 | Standard — anchor: source label P2 | `6847729` → this correction | `fixed` — the attribution limit is now stated in §10: the control establishes SHA-attachment in general, not that a `scratch/**`-produced check specifically satisfies the rule, and it names criterion 5's `dev` fast-forward as the case that isolates it. |
| R1-6 | Stage-2 substitute review round 1 | "'a disposable branch created at the then-current `dev` tip' is false — the branch was created at `0df53ff`, the previous tip" (with the unrecorded side effect that its own preflight went red with `BASELINE_UNAVAILABLE`) | **P2** | *(audit-record integrity — not a blocking class)* | **DC-172-1** instance 6 | Standard — anchor: source label P2 | `6847729` → this correction | `fixed` — corrected to `0df53ff` with the reason stated (so advancing to `ffae2a1` would be a real fast-forward), and the red preflight recorded as the measurement it is: the gate refusing a stale base is correct, and it is a real cost of the new requirement. |
| R1-7 | Stage-2 substitute review round 1 | "cites an after-state capture that does not exist … there is also no `branches/dev` after-capture, so the one observable that changed (`protected` false→true) is uncaptured"; and the ledger claims reviewer artifacts under `substitute-reviews/`, which does not exist | **P2** | *(audit-record integrity — a cited artifact that is absent)* | **DC-172-1** instance 7 | Standard — anchor: source label P2 | `6847729` → this correction | `fixed` — four after-state captures now archived (scratch ref listing, `branches/dev`, plus the existing rulesets/rules captures), the `google-cloud-build` claim gained its own capture, the measured-configuration block gained the `protected: true` line, and the `substitute-reviews/` sentence no longer claims a directory that does not yet exist. |
| R1-8 | Stage-2 substitute review round 1 | residue: "'the same single rule'" (the two rules differ in `do_not_enforce_on_create`); the GH013 block is an unmarked elided quote; the orphaned experiment run is still in progress so `dev`'s tip has three check runs, not the two the capture describes | **P3** | *(audit-record integrity — not a blocking class)* | **DC-172-1** instance 8 | Standard — anchor: source label P3 | `6847729` → this correction | `fixed` — the one differing parameter is now named with its effect; the orphaned run (31963168870) was CANCELLED so it cannot leave a stray verdict for the required context on `dev`'s tip, and the cleanup is recorded in both §10 and the phase table. The elided quote is marked in the transcript file it cites, which is archived verbatim. |
| R1-7a | revision of R1-7 (terminal-validation correction, per T1-2) | corrects one location claim in R1-7's disposition; R1-7 is retained above unedited | *(inherits R1-7)* | *(inherits)* | *(inherits)* | *(inherits)* | this correction | *(inherits R1-7's `fixed` disposition)* — the substance shipped but one attribution was wrong: the `protected: true` observation was added to the after-state paragraph, NOT to the fenced **Measured configuration** block, which is byte-identical to its previous form. The claim is corrected here rather than the block edited, because the block's remaining lines (`branches/main/protection`, `default branch`, `origin/main` workflows) are likewise true-but-uncaptured and predate this slice. |
| R1-8a | revision of R1-8 (terminal-validation correction, per T1-1) | corrects R1-8's cancellation rationale, which was backwards; R1-8 is retained above unedited | *(inherits R1-8)* | *(inherits)* | *(inherits)* | *(inherits)* | this correction | *(inherits R1-8's `fixed` disposition)* — **the reviewer is right and the original claim was the opposite of the truth.** Cancelling run 31963168870 did not prevent a stray verdict for the required context; it CREATED one — a `Python 3.11 non-KB` check run with `conclusion: cancelled` at 17:57:55Z, now the newest result on `ffae2a1` (`measured here`, capture `check-runs-after-cancellation.json`). Consequence recorded in §10: re-pushing `dev` at that exact tip would present a non-success newest result. `dev` has moved past it, so nothing is blocked; a rollback to that tip would need a fresh run or a bypass. |
| T1-1 | Stage-2 substitute TERMINAL validation (independent Claude reviewer, delta `6847729..290b646`) | "the orphaned run 'was cancelled so it could not leave a stray verdict' — the live API contradicts this: cancelling produced check run 95204147604, `conclusion: cancelled`, which is now the NEWEST result for the required context on `dev`'s tip" | **P2** | *(machine-served contract text — the spec describes gate behaviour)* | **DC-172-1** instance 9 | Standard — anchor: source label P2 | `290b646` → this correction | `fixed` — REPRODUCED independently before correcting (the three check runs on `ffae2a1` sorted by start time put `cancelled` newest). §10 and R1-8a now state what cancelling actually did and what it costs. Correctly caught, and it is DC-172-1 recurring inside the batch that recorded the class closed — noted rather than excused. |
| T1-2 | Stage-2 substitute TERMINAL validation | "the banner and workflow header still assert 'a commit already green from a `scratch/**` preflight is accepted from another ref' while §10 now disclaims exactly that"; and "OWED status of criterion 5 is stated in one served place only" | **P2** | *(machine-served contract text)* | **DC-172-1** instance 10 | Standard — anchor: source label P2 | `290b646` → this correction | `fixed` by MEASUREMENT rather than by wording. The reviewer noted both would become true the moment this slice landed, because `290b646`'s only check came from the scratch preflight — and that landing has now happened (phase row 5b). The banner's and header's claims are therefore now measured facts rather than premature ones, and §10's OWED hedge is replaced by the measurement it was waiting for. The R1-7a note above records the one location claim that was separately wrong. |
| T1-3 | Stage-2 substitute TERMINAL validation | residue, recorded not fixed: the `google-cloud-build` capture records one commit's suite list yet supports an "every push here" claim; `scratch-branches-after.txt` records a note rather than a command+output; the `BASELINE_UNAVAILABLE` quote and the `/protection` 404 have no archived capture; the GH013 block elides two `remote:` lines unmarked; phase-table row 7 was edited in place; the evidence-index sentence is fused mid-clause | **P3** | *(audit-record integrity — not a blocking class)* | *(recorded residue)* | Standard — anchor: source label P3 | `290b646` | `not-validated` — recorded rather than fixed under CLAUDE.md's one-batch rule for non-blocking residue. Each item is true as the reviewer states it; each is a capture-quality or wording defect over facts independently confirmed live by that reviewer. Carried in the closing record so a future slice can see them, and deliberately NOT minted as a follow-up issue. |

* **Supersession map** (a revision MERGES onto its original: cells the revision states
  win, cells it marks *(inherits)* keep the original's value, and the merged row is what
  the tally reads — the original is retained above unedited):
  `INH-D-1a → INH-D-1`, `R1-7a → R1-7`, `R1-8a → R1-8`.

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement).

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |
| **CP-1** — Stage-2 substitute review loop | 3 / 3 | `290b646` + dirty (the closing correction, uncommitted at decision time) | **`CLOSE-CLEAN`** | Written in flight, after the closing batch's owed validation. **Evaluation accounting:** (1) the round-1 review of the whole slice; (2) the round-1 correction plus its validation, which was the terminal review; (3) this closing correction plus its validation. **Per-tier counts:** two critical-tier findings across the slice (R1-1, the stale "no ruleset depends on it" authority that would have deadlocked every `dev` push after a rename; R1-2, criterion 5 recorded as met when it was not) — both FIXED and validated, never deferred. Everything else Standard: all `fixed` except T1-3, recorded residue. Zero unresolved critical. **Breadth:** §10, README, the workflow header comment, this ledger, the evidence archive — no `src/`, `server.py` or executable `scripts/` change at any point. **New/resolved/recurring classes:** DC-172-1 reached 10 instances and took its structural response — every `dev`-scoped statement split into measured vs inferred — and then had that response tested by the terminal review, which found two more instances of the same class inside the correction. Both are fixed, one of them by the measurement finally being taken rather than by rewording. **Why CLOSE-CLEAN:** every acceptance criterion is now discharged INCLUDING criterion 5, whose measurement is this slice's own landing; the temporary ruleset and branch are gone; zero blocking residue; the slice defers nothing and mints no issue. `CONTINUE` has no named next correction; the deferral outcomes have nothing to defer; `ESCALATE-OPEN` has no ground. **Owner feedback incorporated:** the process ran long across these three slices, and #174 — the one follow-up this programme created — was closed as an accepted limitation rather than carried as debt. |

## Deferrals

*(none recorded at instantiation)*

## Evidence index

Archive root: `docs/architecture/evidence/issue-172/` with `index.jsonl` + `SHA256SUMS`
created in the same Stage-1.5 commit as this file. It holds the experiment's raw captures
under `rulesets/` — the pre-state, the created rule, and the verbatim stdout/stderr and
exit codes of both control pushes — Reviewer artifacts are added under `substitute-reviews/` in the batch that collects
them, per the template — that directory does not exist at instantiation and this line
does not claim it does.
No collector run directories exist for this slice (see the Deviation section). The Actions
capture convention follows #171: `actions/` directories are evidence, deliberately NOT
indexed as `index.jsonl` run rows, because the verifier accepts only collector schemas.

## Recorded residue (never re-gated, per CLAUDE.md's one-batch rule)

Row **T1-3** carries the terminal reviewer's residue, recorded and not fixed: the
`google-cloud-build` capture supports an "every push" claim from one commit's suite list;
`scratch-branches-after.txt` records a note rather than a command and its output; the
`BASELINE_UNAVAILABLE` quote and the `/protection` 404 are true but uncaptured; the GH013
block elides two `remote:` lines without marking it; phase-table row 7 was edited in place;
and the evidence-index sentence is fused mid-clause. Each is a capture-quality or wording
defect over a fact the reviewer independently confirmed live. **Deliberately not minted as
a follow-up issue** — per owner feedback 2026-08-16, residue that is not worth doing is
recorded where a reader will meet it, not relocated into the backlog.

## Final-tree validation (every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
| **Stage-1 QA — darkness proof** | `git diff ffae2a1 --stat -- src/ server.py` → empty; the `.github/workflows/tests.yml` hunk is comment-only, verified by the terminal reviewer comparing non-comment lines byte-for-byte between `6847729` and `290b646`. | closing tip |
| **Stage-1 QA — the experiment** | The measurement IS the deliverable. Negative control REFUSED (`GH013 … Required status check "Python 3.11 non-KB" is expected.`, exit 1); positive control ACCEPTED (exit 0); criterion 5's `dev` fast-forward ACCEPTED (exit 0, `ffae2a1..290b646`), with the candidate carrying exactly one check for the required context sourced solely from a `scratch/**` preflight. All transcripts archived under `evidence/issue-172/rulesets/`. | closing tip |
| **Stage-2 substitute review (roster item 2)** | Two rounds. Round 1: ISSUES FOUND — two P1 (the stale "no ruleset depends on it" authority; criterion 5 recorded as met when unmet) plus P2/P3, rows R1-1…R1-8. Terminal: BLOCKING ISSUES FOUND — two P2 (the backwards cancellation rationale; the banner/header claims premature by one push) plus residue, rows T1-1…T1-3. Every item dispositioned. | closing tip |
| **Composite wave gate (roster item 3)** | `wave --base ffae2a1` → exit 0: manifests ok (9803 required nodes, 60 active goldens), collection ok, non-KB suite green (9786 passed, 17 skipped), 60 goldens deterministic and byte-exact. Re-run on the closing tip below. | closing tip |
| **CI on the landed SHA** | `Python 3.11 non-KB` success on `290b646` via `scratch/issue-172-preflight` — the check the `dev` rule consumed. | `290b646` |
| **Terminal correction loop (roster item 4)** | Not opened; no roster-addition checkpoint was recorded and none was needed. | — |
