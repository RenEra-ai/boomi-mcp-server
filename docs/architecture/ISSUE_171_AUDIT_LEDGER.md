# Audit ledger — issue #171 (M12.13 follow-up)

Instantiated at Stage-1 step 0 from `templates/AUDIT_LEDGER_TEMPLATE.md`, committed in
the Stage-1.5 baseline commit before the first Stage-2 round.

Conventions inherited from #152's end state: no hand-maintained totals or instance lists
— counts are derived from the rows; every derived field carries its deriving anchor
inline; platform-behaviour claims carry a provenance marker (`measured here` /
`documented, not measured` / `assumption`); measured output is quoted, never re-keyed
from memory; audit-record-integrity findings are non-blocking from round one; diagnostic
codes named anywhere in this file exist in the gate's `DIAGNOSTIC_CODES` and sit inside a
backtick span or fence body.

## Baseline (Stage-1 step 0)

- Issue: #171 — gate cannot run on a scratch branch: no non-PR trigger
  (`push: scratch/**` + `ci --base`)
- Step-0 baseline: `6792d0658b6da7964e35b3c493c8320dee2c1c6a`
- Slice kind: **behaviour-affecting for the gate CLI and CI configuration; DARK to the
  Boomi MCP runtime.** No change under `src/`, `server.py`, `server_http.py`, the golden
  XML corpus, or any MCP-served schema. Stage-1 QA is therefore a darkness proof plus
  live exercise of the changed surface (the gate CLI end-to-end, then the real Actions
  runs), not a live Boomi matrix.
- Artifact trust boundary:
  - **Creates and owns** — the `ci` baseline-selector contract, the `ci_mode` checkout
    invariant, the workflow's two-route dispatch, the manifest transaction, this ledger
    and its evidence archive.
  - **Consumes from outside** — git's ancestry graph (`git merge-base --is-ancestor`),
    GitHub Actions' checkout identity (`GITHUB_SHA`, `GITHUB_ACTIONS`), Actions' event
    and ref variables (`GITHUB_EVENT_NAME`, `GITHUB_EVENT_PATH`, `GITHUB_REF`), and
    `actions/checkout`'s remote-ref materialisation. Every claim about the consumed side
    carries a provenance marker; none is hand-modelled in served text without one.
- Expected defect classes, pre-enumerated so a second instance triggers structurally ON
  ARRIVAL:
  - **DC-A (checkout/evidence binding)** — recurrence of #152's mechanism under a NEW
    runtime authority. Seeded as second-instance-on-arrival; see the defect-class table.
  - **DC-10 (hand-modelled platform behaviour in served text)** — #152 hit this ten
    times. Every GitHub claim in this slice's served prose carries a provenance marker.
  - **hand-enumeration shadowing a derivable authority** — the generic class; the
    `explicit_base is not None` → `context["kind"] == "local"` change is a pre-emptive
    removal of one such second copy.

## Loop roster (fixed BEFORE the first correction; a gate not listed here cannot mint a loop mid-run)

1. **Stage-1 QA** — darkness proof per round (src/golden diff + stash differential) plus
   end-to-end exercise of the changed CLI surface.
2. **Stage-2 repo Codex review** — delta-scoped; every round COLLECTED via the collector,
   never read from `wait`.
3. **§6 architect implementation review** — the wrapping pipeline's declared additive
   gate (implementation-vs-design-plan), in declared order after Stage 2.
4. **Actions rollout gate** — scratch GREEN, seeded scratch RED, final GREEN. This is the
   gate that discharges criteria 4, 5 and 7.
5. **Composite wave gate** — full non-KB suite + every active golden + determinism +
   manifests.
6. **Terminal correction loop** — ONLY via a recorded roster-addition checkpoint.

## Defect-class ledger

A class is a (mechanism, runtime-authority) pair, assigned at reconciliation, revisable
with the original retained.

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| **DC-A** | the tree under test is not bound to the identity the run's authority describes | git ancestry graph + Actions' `GITHUB_SHA` | seeded as **instance 2 on arrival** (instance 1 = #152's push/PR checkout-binding findings, same mechanism under the *event payload* authority); **instance 3 = A6-6**, the widened binding's own missing witness | **Structural fix applied in the first batch**, not an instance patch — see below. A6-6 did not break the invariant; it broke the PROOF of the invariant, and is fixed by completing the witness set rather than by re-narrowing the code |
| **DC-B** | a hand-listed allowlist shadowing a set that is derivable from the filesystem | the audit-ledger filenames present under `docs/architecture`, as globbed by the scanner itself | 1 (S1-1) | structural: the derived set is computed once and consumed by BOTH scans in the scanner; the hand-list keeps only FIXED document names, which do not grow per slice |
| **DC-7** | served prose drifting from the evidence or code it describes | the measured runs, and the implemented CLI | 2 (A6-1, A6-3) | second instance reached at the §6 gate. Structural response: run claims are now single-sourced — §10, README and #152's note all DEFER to this ledger rather than restating a run, so there is one place a demonstration claim can live and one place to correct |
| **DC-10** | hand-modelled GitHub platform behaviour in served text | GitHub's actual semantics | 1 (A6-4) | instance-level: provenance markers added. Carried forward from #152, where this class reached ten instances; the marker convention IS that structural fix, and this slice applies it |
| **DC-12** | a residual claimed as owned by a follow-up | that issue's existence and acceptance criteria | 2 (INH-TC1-2 inherited, A6-2) | #172 filed with number, criteria, reason class and placement before the deferral text was written |
| **DC-13** | an assertion whose scope is wider than the property it claims | the boundary of the thing under assertion (here, the workflow step) | 1 (A6-5) | structural: assert inside the isolated step AND assert absence outside it, so both halves of "exactly one step does this" are bound |

**DC-A structural fix, applied in batch 1** (mandatory on second instance):

- *Invariant, derived from the authority:* every `ci` context binds `HEAD` to the
  platform's `GITHUB_SHA`, and every explicit `ci` baseline must be an ancestor of `HEAD`
  per `git merge-base --is-ancestor`. Neither fact is re-modelled in Python.
- *Sibling sweep* over the authority's full case set — baseline kind × subcommand:

  | | `ci` | `wave` | `manifests` |
  | --- | --- | --- | --- |
  | `push` | clean + platform binding + `after` check | n/a (no event selector) | clean + `after` check |
  | `pull_request` | clean + platform binding + merge identity | n/a | clean + merge identity |
  | `local` | **clean + platform binding + ancestry** (new) | dirty allowed, no ancestry | dirty allowed, no ancestry |

  The `wave`/`manifests` local cells are deliberately unchanged: there the operator chose
  the baseline and the uncommitted work is the subject. That scoping is itself witnessed
  (see the non-vacuity list), so "scoped" is proven rather than asserted.
- *Non-vacuity witnesses* — each constructs a concrete case the invariant excludes:
  dirty tree under `ci --base`; a malformed / wrong / absent `GITHUB_SHA` inside Actions;
  a supplied-but-wrong `GITHUB_SHA` outside Actions; a baseline off this history. Plus
  the two scoping witnesses that the same dirty tree and the same non-ancestor baseline
  are ACCEPTED when the subcommand is not `ci`. Plus — added at the §6 gate, finding
  A6-6 — push and PR contexts under `ci_mode=True`, constructed so the arm itself
  already agrees with the checkout and only the platform binding can refuse.
- *Coverage claim, CORRECTED at the §6 gate:* the authority's case set is the nine cells
  above. An earlier revision of this row claimed all nine were asserted. **That was
  false, and was falsified by measurement, not by reading:** with the platform binding
  moved back under the `local` arm the suite stayed green at 243/243, so the three
  `ci` cells for push and pull_request were unwitnessed. After A6-6's fix the same
  mutant fails. The claim now holds because the mutant that would break it dies.

Second-instance check is run against this table at row-write time.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-TC1-2 | inherited from `ISSUE_152_AUDIT_LEDGER.md` row TC1-2 (not a gate of this slice) | "Capture a green run after reverting the PR seeds" — every seeded run was `pull_request`; the only green run is `push`/BOOTSTRAP, a different baseline path | P2 | capability reachability | DC-12 (a residual claimed as owned by a follow-up; authority = that issue's acceptance criteria) | Standard — anchor: source label P2, and no critical blocking class | inherited at `6792d065` | *(open — discharged by criterion 7(a) landing plus archived scratch GREEN evidence; see the criteria matrix)* |
| S1-1 | Stage-1 QA, full non-KB suite run (no run dir — a local suite execution, not a collected gate) | `test_diagnostic_codes_named_in_the_audit_ledger_exist` failed: "the audit ledger names diagnostic codes the gate cannot emit: ['ISSUE_171_AUDIT_LEDGER']" | *(none — surfaced by the suite, not labelled by a reviewer)* | machine-served schemas/contracts (the scanner is the served checker for every ledger) | **DC-B** instance 1 | Standard — anchor: no source P0/P1/Critical/High label, and no critical blocking class | uncommitted delta at Stage 1 | `fixed` — structurally. The scanner's own comment already required stems to be allowed "by derivation … never by hand-listing", but only its all-ledgers loop did so; the #152-specific scan read a hand-listed copy. The derived set is now computed once and consumed by both. **Sibling sweep:** both 152-specific assertions (unknown-code and bare-token) plus both all-ledgers assertions — four sites, all now reading the one authority. **Non-vacuity witnesses** *(measured here)*: an invented code inside a backtick span in the new ledger is still rejected; a bare real code in its prose is still rejected; the clean ledger passes. |

| A6-1 | §6 architect review, run dir `cdx-gate-review.ZxeOl1`, attestation `attestation.json` (verdict ISSUES FOUND) | "Rollout evidence is claimed before it exists." §10 says scratch RED/GREEN was demonstrated and #152's note marks TC1-2 discharged, but both runs are still pending and the archive holds no Actions logs | *(none)* | machine-served schemas/contracts (served spec + README prose) | **DC-7** (served prose drifting from the evidence; authority = the measured runs) instance 1 | Standard — anchor: no source P0/P1/Critical/High label; served text, not a critical class | `55fd417`..`e541a3d` | `fixed` — every demonstration claim reverted to its true present state. §10 now names the ledger as the single place a run claim may live; gap 4 says "addressed by configuration, see the ledger for run evidence"; #152's note explicitly declines to declare TC1-2 discharged. The claims will be restated only from measured runs. |
| A6-2 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The unrun ruleset and `[skip ci]` work is not validly deferred" — no issue numbers, acceptance criteria, placement or lineage | *(none)* | *(audit-record integrity — NOT a blocking class)* | **DC-12** (a residual claimed as owned by a follow-up; authority = that issue's existence and criteria) instance 1 | Standard — anchor: no source critical label | `e541a3d` | `fixed` — **#172 filed** with per-item acceptance criteria, reason class `blocked-by-mechanism`, milestone M12 and slot "after #171, before M12 close" recorded in its body at filing time. The Deferrals section now cites the number. The finding was exactly right: the prior text claimed "filed follow-ups" that did not exist. |
| A6-3 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The served specification contradicts the implemented CLI" — §4 says local `--base` keeps dirty-tree support though `ci --base` now requires clean; §10 says opening a PR runs the gate though the trigger is removed | *(none)* | machine-served schemas/contracts | **DC-7** instance 2 | Standard — anchor: no source critical label; served contract text | `e541a3d` | `fixed` — §4 now scopes dirty-tree support to `wave`/`manifests --base` specifically and states the binding covers every `ci` run; the PR paragraph is rewritten in the past tense as history, naming trigger removal as its consequence. |
| A6-4 | §6 architect review, `cdx-gate-review.ZxeOl1` | "Platform claims lack the promised provenance discipline, and one is overbroad" — `before` behaviour, workflow discovery, synthetic PR tree; and README says every matching push runs the workflow, contradicting its own skip caveat | *(none)* | machine-served schemas/contracts | **DC-10** (hand-modelled GitHub platform behaviour in served text; authority = GitHub's actual semantics) instance 1 | Standard — anchor: no source critical label; in a blocking class | `e541a3d` | `fixed` — provenance markers added to the `before`-behaviour and workflow-discovery claims and to README's PR-tree claim; README's opening sentence now reads "that **starts** the workflow", with the qualifier's reason stated inline. |
| A6-5 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The single-step workflow test is structurally vacuous" — it searches the entire YAML, so the scratch arm could move into a second step while every assertion still passed | *(none)* | capability reachability (the test is the only thing binding the workflow's shape) | **DC-13** (an assertion whose scope is wider than the property it claims; authority = the step boundary) instance 1 | Standard — anchor: no source critical label | `e541a3d` | `fixed` — the named step is now isolated by indentation-aware slicing and every arm asserted INSIDE it, plus `wave_gate.py` asserted absent from the remainder. **Non-vacuity witness** *(measured here)*: moving the scratch arm into a second unnamed step now fails the test. Implemented by slicing rather than the reviewer's suggested YAML parse — see the refutation note below. |
| A6-6 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The deliberate all-`ci` platform binding lacks push/PR witnesses" — `GITHUB_SHA` is tested only for `kind="local"`, so moving the binding back under the local arm would leave the suite green | *(none)* | capability reachability | **DC-A** instance 3 — the structural fix's own coverage claim was unwitnessed | Standard — anchor: no source critical label. *(Recorded as the most consequential of the six: it falsified this ledger's own coverage claim.)* | `e541a3d` | `fixed` — push and PR witnesses added with `ci_mode=True`, each constructed so the ARM ITSELF is already satisfied (`after` IS the checkout; `event_head` IS the checkout) so only the binding can refuse — a wrong-sha assertion alone would not discriminate, because both arms raise the same code unaided. **Non-vacuity witness** *(measured here)*: before the fix the narrowing mutant left 243/243 green; after it, the mutant dies. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement). A refutation names the disputed claim and
the concrete evidence. An original label is never edited — a revision is a new dated line
with the original retained.

**Partial refutation of A6-5's suggested remedy (the finding itself stands, and is
fixed).** The review proposed "Parse the workflow or isolate the named step"; the parse
half is not available here. PyYAML is declared in neither `requirements.txt` nor
`requirements-dev.txt`, and that file states it is the complete, authoritative set CI
installs. A module-level `import yaml` in the gate's own test module would therefore be a
COLLECTION ERROR on the runner — the precise failure this gate exists to prevent — while
passing locally, because the developer virtualenv happens to carry PyYAML transitively.
The isolate-the-step half of the remedy was taken instead, which closes the same vacuity
with no new dependency. *(Measured here: `pip show pyyaml` reports no declaring package,
and neither requirements file names it.)*

**INH-TC1-2 lineage, carried forward unchanged:** first deferral, reason class
`out-of-scope-by-design`, NOT `window-exhausted`. Its single-use `window-exhausted`
allowance is therefore still unspent.

## Acceptance-criteria evidence matrix

| # | Criterion | Discharged by | State |
| --- | --- | --- | --- |
| 1 | `ci` accepts `--base` as a mutually exclusive alternative to `--github-event`; neither → fail closed | `scripts/wave_gate.py` `build_parser()`: `add_mutually_exclusive_group(required=True)`, mirroring `manifests`. Neither/both → argparse usage error, exit 2, first token `GATE_USAGE_INVALID` | applied |
| 2 | `tests.yml` runs on `scratch/**`; the gate step selects its baseline correctly in a SINGLE step that fails closed on an unrecognised event | one `case`-based step, no step-level `if:`; `dev` → event arm, `scratch/**` → `--base` arm, default → exit 2. Routing measured against a stubbed harness for `dev`, nested `scratch/a/b/c`, `main`, `pull_request`, `workflow_dispatch`, and an empty context *(measured here)* | applied |
| 3 | A test asserts the two selectors are mutually exclusive, with a non-vacuity witness | `test_ci_requires_exactly_one_baseline_selector` — both supplied → refusal; neither → refusal; plus both accepted forms, one of which is threaded through the execution seam | applied |
| 4 | A RED run on a scratch branch reproducing a #152 seeded diagnostic, no PR opened | Actions rollout gate — seed 4 (`minimum_collected` raised, a free raise §5 permits) → `PYTEST_COLLECTION_FLOOR` | *(pending)* |
| 5 | A GREEN run on that same non-`push:[dev]` path | Actions rollout gate — scratch GREEN on the candidate | *(pending)* |
| 6 | Baseline-kind `local` confirmed correct in `ci` mode | the DC-A structural fix above: every `context["kind"]` consumer enumerated and given a verdict, the three `local`-under-`ci` cells hardened, the `wave`/`manifests` cells deliberately unchanged and witnessed | applied |
| 7 | Resolve the `pull_request` merge-base path explicitly | **option (a), per the owner's stated preference**: the `pull_request` trigger is REMOVED from `tests.yml`. `_baseline_from_pull_request()` and its twelve unit tests are retained, so the resolver stays covered while the untested CI arm is eliminated | applied |

## Stage-1 QA (evaluation 1)

**Darkness proof** — the slice is dark to the Boomi MCP runtime. Quoted, not summarised:

```
$ git diff --stat 6792d0658b6da7964e35b3c493c8320dee2c1c6a -- \
    src/ server.py server_http.py tests/fixtures/golden_xml/ \
    tests/fixtures/wave_gate/goldens.jsonl
(no output)
```

Zero changes on every MCP-served surface, so there are no live Boomi scenarios to
exercise. The changed surface is the gate CLI and the workflow, and it is exercised
directly rather than by proxy:

```
$ PYTHONPATH=src python scripts/wave_gate.py manifests --base 6792d065…
wave_gate: baseline 6792d0658b6da7964e35b3c493c8320dee2c1c6a (local)
wave_gate: TOMBSTONE pytest-nodes pytest-009550 owner=repository disposition=n/a
wave_gate: manifests ok (9788 required nodes, 60 active goldens)
```

The manifest transaction is therefore legal against the real baseline, and the active
count is unchanged at 9788 — one retirement, one append.

**Fail-closed witness for the new selector** *(measured here)* — `ci --base` against a
dirty tree:

```
CHECKOUT_EVENT_MISMATCH the worktree is not clean, so the tree under test is not the
committed tree this local run describes:
```

**Workflow routing** *(measured here)* — the single gate step driven against a stubbed
`python`/`git` for seven contexts. `push:refs/heads/dev` selects the event arm;
`push:refs/heads/scratch/171-green` and the nested `push:refs/heads/scratch/a/b/c` both
select the `--base` arm (a shell `case` glob spans `/`, unlike pathname expansion);
`push:refs/heads/main`, `pull_request:refs/pull/5/merge`, `workflow_dispatch:…` and an
empty context each exit 2. A scratch push with `origin/dev` unresolvable exits 2. This is
criterion 2's actual claim, measured rather than asserted.

**Suite** — full non-KB suite on the 3.12 local interpreter. First execution surfaced one
finding (row S1-1), fixed structurally; re-run green at `9771 passed, 17 skipped`.

**The new selector, end to end on the committed tree** *(measured here — exit status
captured directly, not inferred from a later line)*:

```
$ PYTHONPATH=src python scripts/wave_gate.py ci --base 6792d065…
wave_gate: baseline 6792d0658b6da7964e35b3c493c8320dee2c1c6a (local)
wave_gate: TOMBSTONE pytest-nodes pytest-009550 owner=repository disposition=n/a
wave_gate: manifests ok (9788 required nodes, 60 active goldens)
wave_gate: collection ok (9788 tests)
wave_gate: non-KB suite green (9771 passed, 17 skipped, cap 30)
PLAN_FINGERPRINT_PENDING issue=#153
```

The process exit status was **0**, captured directly from the invocation rather than
inferred from the last line printed. (An earlier capture piped the run through `tail`,
which discards the status and truncated the header; that measurement was re-taken rather
than reasoned around.)

This is criterion 1 and criterion 6 discharged locally on the exact code path the
`scratch/**` runner executes: baseline kind `local`, the checkout binding satisfied, the
manifest transition legal, collection at the floor, and the suite green. The
plan-fingerprint line is the #153 seam reporting pending, which is not fatal without
`--require-plan-fingerprint`.

## Stage-2 repo Codex review (evaluation 1)

Round 1 — run directory `cdx-review.oDMlDK`, archived at
`docs/architecture/evidence/issue-171/commit-reviews/cdx-review.oDMlDK/`, collected via
the collector (never read from `wait`), teardown `confirmed stopped`.

```
STATUS: completed
SCOPE: branch diff against 6792d0658b6da7964e35b3c493c8320dee2c1c6a (6792d06)
       head=55fd417de75ecdca4e12f6fc499cb1bb4ac6971c dirty=false
```

Verbatim result: "No actionable correctness issues were found in the baseline selection,
checkout binding, ancestry validation, workflow routing, or associated test updates."

Zero findings, so no correction and no new finding rows. Not a checkpoint (evaluation 1
of 3).

## §6 architect implementation review (evaluation 1)

Round 1 — run directory `cdx-gate-review.ZxeOl1`, gate `review`, collected via
`gate-attest collect` with `ok:true`, `stopped:true`, and the design plan's bytes
verified present in the prompt (the collector checks this, so "reviewed against the
plan" is a checkable claim rather than an assertion).

Verdict: **ISSUES FOUND** — six findings, all reconciled above as rows A6-1 … A6-6, all
`fixed`, none refuted outright. One suggested REMEDY was partially refuted on evidence
(A6-5's YAML parse) while its finding was accepted and fixed another way.

The review explicitly confirmed what it did NOT find, which is worth recording because it
bounds the fixes: the Python invariant itself, the workflow routing, and the manifest
transaction were each checked and found correct — including that a shell `case` glob
matches nested `scratch/a/b/c` the way the `branches:` filter does, and that the
`9788 + 1 − 1 = 9788` arithmetic and unchanged floors are right.

Two of the six are worth singling out as the ones a human should care about:

- **A6-6 falsified this ledger's own coverage claim.** The structural fix's invariant was
  correct, but its proof was not; the mutation test is what settled it. A review that only
  read the code would have agreed with the ledger.
- **A6-2 caught a deferral citing an issue that did not exist.** The completion policy
  requires the follow-up to be filed BEFORE the deferral is recorded, precisely so this
  cannot happen; it happened anyway, and the gate caught it.

## Checkpoints (written IN FLIGHT at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

Each rationale records per-tier counts and breadth, new/resolved/recurring defect classes
derived from the rows, the trend vector, explicit rule-outs of the other outcomes, and a
NAMED finite next correction.

## Deferrals

Pointer-only — reason class, placement and lineage live on the finding row and in the
filed issue.

Two items are deferred out of this slice, both to **already-filed issue #172**
(*"required-status viability and the `[skip ci]` hole are undecided pending a measured
ruleset experiment"*), both reason class **`blocked-by-mechanism`**, both **first
deferral — NOT `window-exhausted`**, placement **M12 milestone, after #171 lands and
before M12 close** (recorded in #172's body at filing time, which is also where the
per-item acceptance criteria are enumerated):

1. **The required-status-check measurement** — #172 item 1. Deciding whether a ruleset
   requiring `Python 3.11 non-KB` is viable requires attaching an enforcing rule to a
   live branch with repository-admin authority: the blocking mechanism. The issue asks
   for the question to be decided *against measurement, not predicted*, and predicting
   it is the exact error §10 has already made twice. The experiment is specified in
   §10 and reproduced as #172's acceptance criteria. This is NOT one of #171's
   acceptance criteria 1–7.
2. **The `[skip ci]` hole** — #172 item 2. A skipped workflow cannot repair its own
   absence, so the only mechanism that closes it is the same repository rule as (1);
   #172 item 1's negative control IS this hole's test, which is why the two are filed
   together rather than separately. Recorded as an explicitly tracked residual in §10
   gap 1, not as an oversight.

Both deferrals were recorded at the §6 architect-review reconciliation, and #172 existed
before this text was written — a deferral may not cite an unfiled issue. The checkpoint
row governing them is in the Checkpoints table below.

## Evidence index

Collected run directories are archived (byte-verified, allowlisted sidecars) under
`docs/architecture/evidence/issue-171/` with `index.jsonl` + `SHA256SUMS`, in the batch
that collects them. Run citations always use the COMPLETE run-dir name
(`cdx-review.<suffix>` / `cdx-gate-review.<suffix>`, backticked).

Actions evidence (run metadata, raw logs, the quoted gate stderr, and the empty
pull-request queries proving no PR was opened) is archived under the same tree and covered
by `SHA256SUMS`, but is deliberately NOT indexed as `index.jsonl` run rows: the verifier
accepts only the `commit-review-collect` and `gate-attest` collector schemas, and a row
shaped like a collector row without a collector behind it is precisely the fabrication
this archive exists to prevent.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
