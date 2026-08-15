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
| **DC-A** | the tree under test is not bound to the identity the run's authority describes | git ancestry graph + Actions' `GITHUB_SHA` | seeded as **instance 2 on arrival** (instance 1 = #152's push/PR checkout-binding findings, same mechanism under the *event payload* authority) | **Structural fix applied in the first batch**, not an instance patch — see below |
| **DC-B** | a hand-listed allowlist shadowing a set that is derivable from the filesystem | the audit-ledger filenames present under `docs/architecture`, as globbed by the scanner itself | 1 (S1-1) | structural: the derived set is computed once and consumed by BOTH scans in the scanner; the hand-list keeps only FIXED document names, which do not grow per slice |

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
  are ACCEPTED when the subcommand is not `ci`.
- *Coverage claim:* the authority's case set is the nine cells above; all nine are
  asserted, six by the pre-existing push/PR matrix and three by this slice's additions.

Second-instance check is run against this table at row-write time.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-TC1-2 | inherited from `ISSUE_152_AUDIT_LEDGER.md` row TC1-2 (not a gate of this slice) | "Capture a green run after reverting the PR seeds" — every seeded run was `pull_request`; the only green run is `push`/BOOTSTRAP, a different baseline path | P2 | capability reachability | DC-12 (a residual claimed as owned by a follow-up; authority = that issue's acceptance criteria) | Standard — anchor: source label P2, and no critical blocking class | inherited at `6792d065` | *(open — discharged by criterion 7(a) landing plus archived scratch GREEN evidence; see the criteria matrix)* |
| S1-1 | Stage-1 QA, full non-KB suite run (no run dir — a local suite execution, not a collected gate) | `test_diagnostic_codes_named_in_the_audit_ledger_exist` failed: "the audit ledger names diagnostic codes the gate cannot emit: ['ISSUE_171_AUDIT_LEDGER']" | *(none — surfaced by the suite, not labelled by a reviewer)* | machine-served schemas/contracts (the scanner is the served checker for every ledger) | **DC-B** instance 1 | Standard — anchor: no source P0/P1/Critical/High label, and no critical blocking class | uncommitted delta at Stage 1 | `fixed` — structurally. The scanner's own comment already required stems to be allowed "by derivation … never by hand-listing", but only its all-ledgers loop did so; the #152-specific scan read a hand-listed copy. The derived set is now computed once and consumed by both. **Sibling sweep:** both 152-specific assertions (unknown-code and bare-token) plus both all-ledgers assertions — four sites, all now reading the one authority. **Non-vacuity witnesses** *(measured here)*: an invented code inside a backtick span in the new ledger is still rejected; a bare real code in its prose is still rejected; the clean ledger passes. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement). A refutation names the disputed claim and
the concrete evidence. An original label is never edited — a revision is a new dated line
with the original retained.

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
finding (row S1-1), fixed structurally; re-run recorded in the final-tree table.

## Checkpoints (written IN FLIGHT at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

Each rationale records per-tier counts and breadth, new/resolved/recurring defect classes
derived from the rows, the trend vector, explicit rule-outs of the other outcomes, and a
NAMED finite next correction.

## Deferrals

Pointer-only — reason class, placement and lineage live on the finding row and in the
filed issue.

Two dispositions this slice records as **out of its own scope and carried by filed
follow-ups**, both reason class `blocked-by-mechanism`:

1. **The required-status-check measurement.** Deciding whether a ruleset requiring
   `Python 3.11 non-KB` is viable requires attaching an enforcing rule to a live branch
   with repository-admin authority. The issue asks for the question to be decided
   *against measurement, not predicted*; predicting it is the exact error §10 has already
   made twice. The experiment is specified in spec §10 steps 1–6 and left unrun. This is
   NOT one of acceptance criteria 1–7.
2. **The `[skip ci]` hole.** A skipped workflow cannot repair its own absence, so the
   only mechanism that closes it is the same repository rule as (1). Recorded as an
   explicitly tracked residual in §10 gap 1, not as an oversight.

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
