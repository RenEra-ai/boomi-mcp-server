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

| Class | Mechanism | Runtime authority | Instances (derived from the rows, revisions applied) | Resolution |
| --- | --- | --- | --- | --- |
| **DC-A** | the tree under test is not bound to the identity the run's authority describes | git ancestry graph + Actions' `GITHUB_SHA` | **3** — instance 1 = #152's push/PR checkout-binding findings (same mechanism, event-payload authority), instance 2 = seeded on arrival, instance 3 = A6-6 | structural fix applied in batch 1, see below. A6-6 broke the PROOF, not the invariant, and is fixed by completing the witness set |
| **DC-B** | a hand-listed allowlist shadowing a set derivable from the filesystem | the audit-ledger filenames on disk, as globbed by the scanner | **1** — S1-1 | structural: the derived set is computed once and consumed by both scans; the hand-list keeps only FIXED document names |
| **DC-7** | served prose drifting from the evidence or code it describes | the measured runs, and the implemented CLI | **7** — A6-1, A6-3, R2-2, R2-5, R3-5, AR-1, AR-2 | structural response strengthened twice: run claims single-sourced to this ledger (after instance 2), then — because R3-5 found the same claim surviving in a code comment — a corrected claim requires a **sibling sweep of every copy**, prose and code alike |
| **DC-10** | hand-modelled GitHub platform behaviour in served text | GitHub's actual semantics | **2** — A6-4, AR-3 | provenance markers added. #152 reached ten instances of this; the marker convention IS the structural fix, applied here |
| **DC-12** | a residual claimed as owned by a follow-up | that issue's existence and acceptance criteria | **6** — INH-TC1-2, A6-2, R2-3, R3-1, R4-1, R5-1 | structural response built in three parts as instances arrived: #172 filed BEFORE the deferral text (instance 2), the per-item enumeration D-1/D-2 (instance 3), and a checkpoint that actually precedes the deferral it authorises (instances 4–5 → **CP-2**). **D-1/D-2 are NOT instances** — their revisions D-1a/D-2a record why: a deferral whose residual IS owned by a filed issue with criteria is the condition this class detects the ABSENCE of |
| **DC-13** | an assertion whose scope is wider than the property it claims | the boundary of the thing under assertion | **3** — A6-5, R2-1, R3-2 | instances 2 and 3 each arrived inside the previous instance's own fix. Final structural response: the step boundary is derived from indentation (the real block structure), AND the property carries a committed fixture that fails under the old predicate — a performed mutation is not a witness |
| **DC-14** | a derived field read from a copy rather than from its source | the source run directory the copy was made from | **1** — R2-4 | fix plus a recorded `started_at_provenance` naming the authoritative source |
| **DC-15** | a support run debited to the wrong logical loop | which loop caused the correction | **2** — R3-3, R4-3 | second instance was the same rebilling left half-done. Structural response: a rebilling sweeps heading, archive row AND finding rows together, because the derived count reads the last of those |
| **DC-16** | a derived aggregate not recomputed after its source rows changed | the finding rows | **3** — R3-4, R4-4, R5-3 | second instance was two residual disagreements after the first recompute. Structural response: this table is now REGENERATED from the rows by a script at edit time — the counts above were produced that way, not typed |
| **DC-17** | an archive row claiming a ledger citation the ledger does not make | the ledger text | **1** — R4-2 | the scanner enforces that a CITED run exists; it cannot enforce that a claimed citation was made. Closed by hand here, and recorded as a known asymmetry |
| **DC-19** | a required roster gate treated as satisfied by a DIFFERENT gate's run | the loop roster | **1** — R5-2 | the §6 architect gate was replayed delta-scoped on the corrected tree. Recorded as its own class because it is not a record defect: a gate that never ran cannot be repaired by describing it accurately |
| **DC-18** | an append-only record edited in place | the ledger's own append-only contract | **2** — R4-5, R5-4 | instance 2 was made WHILE fixing instance 1: D-1/D-2 were restored correctly and the R2 rows touched in the same batch were not. Structural response: an append-only correction now sweeps every row the batch touched, and revisions are appended (D-1a/D-2a, R2-1a…R2-5a) rather than folded in |

**How these counts are derived — inputs declared, so the tally is reproducible.**
The count for a class is `len(seed[class]) + len(rows carrying that class, revisions applied)`.

* **Seed input** (instances with no finding row of their own, because they predate this
  slice or were recorded on arrival): `DC-A: 2` — instance 1 is #152's push/PR
  checkout-binding findings under the event-payload authority, instance 2 is the
  on-arrival seeding recorded in the Baseline section. Every other class has `seed = 0`.
* **Row input:** each row's defect-class cell.
* **Supersession map** (a revision MERGES onto its original: cells the revision states
  win, cells it marks *(inherits)* keep the original's value, and the merged row is what
  the tally reads — the original is retained above unedited):
  `D-1a → D-1`, `D-2a → D-2`, `R2-1a → R2-1` … `R2-5a → R2-5`. So D-1/D-2 contribute
  nothing (their revisions state *not an instance*), while R2-1…R2-5 still contribute
  their inherited classes and R2-3 contributes the revised ordinal. A first attempt at
  this rule said a superseded row "contributes nothing", which silently dropped four
  real instances — merge, not replace.

An earlier revision stated the rule without its seed input, so DC-A's printed 3 could not
be reproduced from the ledger (it regenerated as 1). That was round-5 finding R5-3.

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
| INH-TC1-2 | inherited from `ISSUE_152_AUDIT_LEDGER.md` row TC1-2 (not a gate of this slice) | "Capture a green run after reverting the PR seeds" — every seeded run was `pull_request`; the only green run is `push`/BOOTSTRAP, a different baseline path | P2 | capability reachability | DC-12 (a residual claimed as owned by a follow-up; authority = that issue's acceptance criteria) | Standard — anchor: source label P2, and no critical blocking class | inherited at `6792d065`; discharged at `15e922a` | **`fixed`** — both halves now hold. **Criterion 7(a):** the `pull_request` trigger is removed from `tests.yml`, so the never-observed-green merge-base arm is eliminated by design rather than carried; `_baseline_from_pull_request()` and its twelve unit tests are retained, so the resolver stays covered while the untested CI arm is gone. **Criterion 5:** run 31911864696 is a GREEN run on the non-`push:[dev]` path — the first ever recorded — with baseline kind `(local)`. TC1-2's original observation was that *"no non-`push` path has ever been observed reaching green"*; that is no longer true, and the run is archived rather than merely cited. Lineage closes at first deferral: its single-use `window-exhausted` allowance was never spent. |
| S1-1 | Stage-1 QA, full non-KB suite run (no run dir — a local suite execution, not a collected gate) | `test_diagnostic_codes_named_in_the_audit_ledger_exist` failed: "the audit ledger names diagnostic codes the gate cannot emit: ['ISSUE_171_AUDIT_LEDGER']" | *(none — surfaced by the suite, not labelled by a reviewer)* | machine-served schemas/contracts (the scanner is the served checker for every ledger) | **DC-B** instance 1 | Standard — anchor: no source P0/P1/Critical/High label, and no critical blocking class | uncommitted delta at Stage 1 | `fixed` — structurally. The scanner's own comment already required stems to be allowed "by derivation … never by hand-listing", but only its all-ledgers loop did so; the #152-specific scan read a hand-listed copy. The derived set is now computed once and consumed by both. **Sibling sweep:** both 152-specific assertions (unknown-code and bare-token) plus both all-ledgers assertions — four sites, all now reading the one authority. **Non-vacuity witnesses** *(measured here)*: an invented code inside a backtick span in the new ledger is still rejected; a bare real code in its prose is still rejected; the clean ledger passes. |

| A6-1 | §6 architect review, run dir `cdx-gate-review.ZxeOl1`, attestation `attestation.json` (verdict ISSUES FOUND) | "Rollout evidence is claimed before it exists." §10 says scratch RED/GREEN was demonstrated and #152's note marks TC1-2 discharged, but both runs are still pending and the archive holds no Actions logs | *(none)* | machine-served schemas/contracts (served spec + README prose) | **DC-7** (served prose drifting from the evidence; authority = the measured runs) instance 1 | Standard — anchor: no source P0/P1/Critical/High label; served text, not a critical class | `55fd417`..`e541a3d` | `fixed` — every demonstration claim reverted to its true present state. §10 now names the ledger as the single place a run claim may live; gap 4 says "addressed by configuration, see the ledger for run evidence"; #152's note explicitly declines to declare TC1-2 discharged. The claims will be restated only from measured runs. |
| A6-2 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The unrun ruleset and `[skip ci]` work is not validly deferred" — no issue numbers, acceptance criteria, placement or lineage | *(none)* | *(audit-record integrity — NOT a blocking class)* | **DC-12** (a residual claimed as owned by a follow-up; authority = that issue's existence and criteria) instance 1 | Standard — anchor: no source critical label | `e541a3d` | `fixed` — **#172 filed** with per-item acceptance criteria, reason class `blocked-by-mechanism`, milestone M12 and slot "after #171, before M12 close" recorded in its body at filing time. The Deferrals section now cites the number. The finding was exactly right: the prior text claimed "filed follow-ups" that did not exist. |
| A6-3 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The served specification contradicts the implemented CLI" — §4 says local `--base` keeps dirty-tree support though `ci --base` now requires clean; §10 says opening a PR runs the gate though the trigger is removed | *(none)* | machine-served schemas/contracts | **DC-7** instance 2 | Standard — anchor: no source critical label; served contract text | `e541a3d` | `fixed` — §4 now scopes dirty-tree support to `wave`/`manifests --base` specifically and states the binding covers every `ci` run; the PR paragraph is rewritten in the past tense as history, naming trigger removal as its consequence. |
| A6-4 | §6 architect review, `cdx-gate-review.ZxeOl1` | "Platform claims lack the promised provenance discipline, and one is overbroad" — `before` behaviour, workflow discovery, synthetic PR tree; and README says every matching push runs the workflow, contradicting its own skip caveat | *(none)* | machine-served schemas/contracts | **DC-10** (hand-modelled GitHub platform behaviour in served text; authority = GitHub's actual semantics) instance 1 | Standard — anchor: no source critical label; in a blocking class | `e541a3d` | `fixed` — provenance markers added to the `before`-behaviour and workflow-discovery claims and to README's PR-tree claim; README's opening sentence now reads "that **starts** the workflow", with the qualifier's reason stated inline. |
| A6-5 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The single-step workflow test is structurally vacuous" — it searches the entire YAML, so the scratch arm could move into a second step while every assertion still passed | *(none)* | capability reachability (the test is the only thing binding the workflow's shape) | **DC-13** (an assertion whose scope is wider than the property it claims; authority = the step boundary) instance 1 | Standard — anchor: no source critical label | `e541a3d` | `fixed` — the named step is now isolated by indentation-aware slicing and every arm asserted INSIDE it, plus `wave_gate.py` asserted absent from the remainder. **Non-vacuity witness** *(measured here)*: moving the scratch arm into a second unnamed step now fails the test. Implemented by slicing rather than the reviewer's suggested YAML parse — see the refutation note below. |
| A6-6 | §6 architect review, `cdx-gate-review.ZxeOl1` | "The deliberate all-`ci` platform binding lacks push/PR witnesses" — `GITHUB_SHA` is tested only for `kind="local"`, so moving the binding back under the local arm would leave the suite green | *(none)* | capability reachability | **DC-A** instance 3 — the structural fix's own coverage claim was unwitnessed | Standard — anchor: no source critical label. *(Recorded as the most consequential of the six: it falsified this ledger's own coverage claim.)* | `e541a3d` | `fixed` — push and PR witnesses added with `ci_mode=True`, each constructed so the ARM ITSELF is already satisfied (`after` IS the checkout; `event_head` IS the checkout) so only the binding can refuse — a wrong-sha assertion alone would not discriminate, because both arms raise the same code unaided. **Non-vacuity witness** *(measured here)*: before the fix the narrowing mutant left 243/243 green; after it, the mutant dies. |

| R2-1 | Stage-2 repo review round 2, run dir `cdx-review.8WTj1E` | "[P2] Stop the step slice at every structural dedent" — a later valid job indenting its `steps` sequence more deeply does not terminate the loop, so both gate invocations land inside `step` and none in `outside` | **P2** | capability reachability | **DC-13** instance 2 — the same class as A6-5, recurring in A6-5's own fix | Standard — anchor: source label P2; not in a critical class | `5fa143d` | `fixed` — the loop now terminates on ANY nonblank dedent. **Non-vacuity witness** *(measured here)*: a valid two-job workflow with the scratch arm in the second job passed before the fix and fails after it. Second instance of DC-13 → the structural response is that the slice's terminator is now derived from indentation alone (the actual block boundary) rather than from a guessed token shape. |
| R2-2 | Stage-2 round 2, `cdx-review.8WTj1E` | "[P2] Keep A6-1 open until the discharge claim is removed" — `ISSUE_152_AUDIT_LEDGER.md` still says TC1-2 "was discharged" while its new lines say the opposite | **P2** | machine-served schemas/contracts | **DC-7** instance 3 | Standard — anchor: source label P2 | `5fa143d` | `fixed` — the note's opening now says the residual is "OWNED and tracked", not "discharged", and points at its own closing paragraph. A6-1's fix had corrected the new text but left the sentence that introduced it. |
| R2-3 | Stage-2 round 2, `cdx-review.8WTj1E` | "[P2] Add finding and checkpoint rows for the deferrals" — #172 alone is insufficient; neither residual has its own finding row and the Checkpoints table is empty despite the text claiming a governing row exists | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-12** instance 5 | Standard — anchor: source label P2 | `5fa143d` | `fixed` — rows **D-1** and **D-2** added with reason class, placement and lineage; checkpoint **CP-1** added with its full recorded decision. The prior text asserted a checkpoint row that did not exist, which is the same claimed-before-it-exists shape as A6-1. |
| R2-4 | Stage-2 round 2, `cdx-review.8WTj1E` | "[P3] Correct the architect review's impossible chronology" — `started_at` 20:15:19Z but `collected_at` 20:08:07.903Z, so the review was collected before it started | **P3** | *(audit-record integrity — NOT a blocking class)* | **DC-14** (a derived field read from the wrong source; authority = the source run directory) instance 1 | Standard — anchor: source label P3 | `5fa143d` | `fixed` — `started_at` recovered from the SOURCE run dir (`19:57:09Z`); the archived copy's mtime is the copy time, which is what the earlier revision read. A `started_at_provenance` field and a limitation now record which source is authoritative, and all three index rows were re-checked for chronological sanity. |
| R2-5 | Stage-2 round 2, `cdx-review.8WTj1E` | "[P3] Correct the PyYAML availability claim" — `requirements.txt` pins FastMCP 3.1.1, which declares PyYAML unconditionally, so importing `yaml` would not cause the asserted collection error | **P3** | machine-served schemas/contracts (a recorded technical claim in the audit record) | **DC-7** instance 4 | Standard — anchor: source label P3 | `5fa143d` | `fixed` — **my refutation was wrong and is withdrawn**; see the withdrawal note below the rows. The implementation choice stands on restated, true grounds. *(Measured here: `importlib.metadata.requires("fastmcp")` → `pyyaml<7.0,>=6.0`, unconditional.)* |
| D-1 | issue #171 body, "Also unblocks: deciding on a required status check" (a scope item of the slice, not a gate finding) | "Whether this issue changes that depends on the trigger actually chosen and should be decided **against measurement**, not predicted" | *(none — an issue scope item)* | capability reachability | **DC-12** instance 3 | Standard — anchor: no source critical label; not in a critical class | not applicable (no code delta) | **`deferred`** → **#172 item 1**, reason class **`blocked-by-mechanism`** (the blocking mechanism: only a repository ruleset can answer it, and creating an enforcing rule needs repository-admin authority on a live branch — unavailable from inside the tree), placement **M12, after #171 lands and before M12 close**, recorded in #172's body at filing time. Lineage: **first deferral, NOT `window-exhausted`**. Governed by checkpoint CP-1 below. |
| D-2 | issue #171 body, "a push whose head commit message carries `[skip ci]` starts no run at all … Worth deciding here whether that hole is acceptable" | the `[skip ci]` hole leaves a pushed tip unchecked | *(none — an issue scope item)* | capability reachability | **DC-12** instance 4 | Standard — anchor: no source critical label | not applicable (no code delta) | **`deferred`** → **#172 item 2**, reason class **`blocked-by-mechanism`** (a skipped workflow cannot repair its own absence; the only closing mechanism is the same ruleset as D-1, which is why #172 item 1's negative control IS this hole's test), placement **M12, after #171 lands and before M12 close**. Lineage: **first deferral, NOT `window-exhausted`**. Governed by checkpoint CP-1 below. |

| R3-1 | §6 architect loop, support review, run dir `cdx-review.BGzeM0` | "[P2] Re-record the deferrals at a committed checkpoint" — at `5fa143d` the checkpoint table was empty, so D-1/D-2 and #172 could not cite an already-committed decision; and the subsequent review of `5fa143d` returned five findings, contradicting "current-tip validation complete" | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-12** instance 4 | Standard — anchor: source label P2 | `d6674cd` | `fixed` — CP-1 marked not-validly-authorized and RETAINED unedited; **CP-2** recorded below at a tip whose validation actually completed. The finding was exactly right: a checkpoint written into the commit after the SHA it names cannot be what a deferral in that same commit cited. |
| R3-2 | §6 architect loop, support review, run dir `cdx-review.BGzeM0` | "[P2] Commit a non-vacuity witness for the new slice boundary" — the real workflow has one job with the gate step last, so old and new predicates both run to end-of-file; reverting the fix leaves the test green | **P2** | capability reachability | **DC-13** instance 3 | Standard — anchor: source label P2 | `d6674cd` | `fixed` — `_split_out_gate_step()` extracted to module scope and `test_the_step_slice_ends_at_a_dedented_second_job` added, carrying a synthetic two-job fixture. **Non-vacuity witness** *(measured here)*: reverting the predicate now fails that committed test, where before it failed nothing. Third DC-13 instance, and the one that finally made the property durable rather than performed. |
| R3-3 | §6 architect loop, support review, run dir `cdx-review.BGzeM0` | "[P2] Bill the correction review to the architect loop" — the review validates the §6 correction but was archived as Stage-2 evaluation 2, shifting checkpoint counts and hiding R2-1…R2-5 from the §6 loop's accounting | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-15** (a support run debited to the wrong logical loop; authority = which loop caused the correction) instance 1 | Standard — anchor: source label P2 | `d6674cd` | `fixed` — the section heading and the archive's `logical_loop` were corrected at round 3; the finding ROWS were rebilled at round 4 (see R4-3), which is where the correction was actually completed. |
| R3-4 | §6 architect loop, support review, run dir `cdx-review.BGzeM0` | "[P2] Recompute defect-class state from the added rows" — the table reported two instances each for DC-7 and DC-12 while the rows identified more | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-16** (a derived aggregate not recomputed after its source rows changed; authority = the finding rows) instance 1 | Standard — anchor: source label P2 | `d6674cd` | `fixed` — counts recomputed from the rows and the D-1/D-2 mis-assignment corrected. Completed at round 4 per R4-4, which caught two residual disagreements. |
| R3-5 | §6 architect loop, support review, run dir `cdx-review.BGzeM0` | "[P3] Remove the withdrawn PyYAML claim from the test comment" — the ledger records the withdrawal but `tests/test_wave_gate.py` still said PyYAML is absent and importing it would break collection | **P3** | machine-served schemas/contracts | **DC-7** instance 5 | Standard — anchor: source label P3 | `d6674cd` | `fixed` — the comment now states PyYAML IS importable and that the textual slice is a preference, not a necessity. A sibling-sweep miss: R2-5's fix corrected the ledger copy of the claim and left the code copy, which is why DC-7's resolution now requires sweeping every copy. |
| R4-1 | §6 architect loop, support review, run dir `cdx-review.AAM0JP` | "[P2] Keep the deferrals unresolved until CP-2 exists" — D-1/D-2 retained a `deferred` disposition while saying authorization was pending, and the Deferrals prose still said CP-1 governs them | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-12** instance 5 | Standard — anchor: source label P2 | `fea26a6` | `fixed` — **CP-2 now exists** and is recorded below at a validated tip; the Deferrals prose cites it; D-1/D-2 keep their ORIGINAL text with superseding revision rows D-1a/D-2a carrying the authorization. The deferrals are authorized by CP-2, not by the superseded CP-1. |
| R4-2 | §6 architect loop, support review, run dir `cdx-review.AAM0JP` | "[P2] Record every round-3 finding in the ledger" — the archive row set `ledger_cited: true` and referenced R3-1…R3-5, but the ledger neither cited `cdx-review.BGzeM0` nor held those rows | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-17** (an archive row claiming a ledger citation the ledger does not make; authority = the ledger text) instance 1 | Standard — anchor: source label P2 | `fea26a6` | `fixed` — rows R3-1…R3-5 added above, the round-3 section added below citing the COMPLETE run-dir name, and the same done for round 4. The archive scanner enforces that a CITED run exists; it cannot enforce that a claimed citation was made, which is the hole this finding closes by hand. |
| R4-3 | §6 architect loop, support review, run dir `cdx-review.AAM0JP` | "[P2] Finish rebilling the R2 finding rows" — heading and index were rebilled to §6 but rows R2-1…R2-5 still named Stage-2 as their source, so derived counts kept the misbilling | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-15** instance 2 | Standard — anchor: source label P2 | `fea26a6` | `fixed` — all five R2 rows rebilled to the §6 loop with their ORIGINAL wording retained inline as revision history. Second DC-15 instance: the structural response is that a loop rebilling must now sweep heading, archive row AND finding rows together, since the count is derived from the last of those. |
| R4-4 | §6 architect loop, support review, run dir `cdx-review.AAM0JP` | "[P2] Make the defect-class state derivable from its rows" — DC-7 cited R3-5 while its count stopped at four, and DC-12 called R2-3 instance 3 although the R2-3 row said instance 5 | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-16** instance 2 | Standard — anchor: source label P2 | `fea26a6` | `fixed` — the R2-3 row's ordinal revised to 3 with the original retained, and the class table recomputed to agree with every row. Second DC-16 instance: the structural response is that the table is now regenerated from the rows by a script at edit time rather than hand-updated, and the counts below were produced that way. |
| R4-5 | §6 architect loop, support review, run dir `cdx-review.AAM0JP` | "[P3] Retain the original D-1 and D-2 rows" — they were overwritten in place to change defect class and authorization, despite the append-only contract; git history is not the in-tree audit record | **P3** | *(audit-record integrity — NOT a blocking class)* | **DC-18** (an append-only record edited in place; authority = the ledger's own append-only contract) instance 1 | Standard — anchor: source label P3 | `fea26a6` | `fixed` — D-1/D-2 restored to their original committed text, with revisions carried by new rows D-1a/D-2a. Correct on the principle, and it applies to this ledger's own conventions section: *"An original label is never edited — a revision is a new dated line with the original retained."* |
| D-1a | revision of D-1 (round 4, per R4-5) | supersedes D-1's defect class and checkpoint authorization; D-1's original text is retained above unedited | *(inherits D-1)* | capability reachability | **not an instance** — revised from "DC-12 instance 3": D-1 is a deferral RECORD whose residual IS owned by a filed issue carrying acceptance criteria, which is the condition DC-12 detects the ABSENCE of. Counting it manufactured a false recurrence. | Standard — unchanged | `fea26a6` | **`deferred`** → **#172 item 1**, authorized by **CP-2** (not the superseded CP-1). Reason class, placement and lineage are unchanged from D-1. |
| D-2a | revision of D-2 (round 4, per R4-5) | supersedes D-2's defect class and checkpoint authorization; D-2's original text is retained above unedited | *(inherits D-2)* | capability reachability | **not an instance** — revised from "DC-12 instance 4"; see D-1a | Standard — unchanged | `fea26a6` | **`deferred`** → **#172 item 2**, authorized by **CP-2**. Reason class, placement and lineage unchanged from D-2. |

| R5-1 | §6 architect loop, support review, run dir `cdx-review.OmOkZe` | "[P2] Record CP-2 only after validating the correction" — CP-2 is introduced in `832d6a6` but names `fea26a6`, repeating CP-1's back-dating, so D-1a/D-2a cannot cite it as an already-existing current-tip-validated checkpoint | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-12** instance 6 | Standard — anchor: source label P2 | `832d6a6` | **`not-validated` — OPEN.** CP-2 is superseded and NO replacement checkpoint exists. There is therefore currently **no valid checkpoint authorizing D-1a/D-2a**, and the slice cannot close on them. What is owed, in this order: fix the outstanding findings, validate, obtain a CLEAN architect-gate replay, and only then record a checkpoint — in its own commit, naming a tip that has actually passed — followed by the deferral revisions citing it. An earlier revision of this cell described that sequence as already done; it was not, which is finding AR-1 of the replay. |
| R5-2 | §6 architect loop, support review, run dir `cdx-review.OmOkZe` | "[P2] Replay the originating architect gate before proceeding" — the archive holds only the pre-correction `cdx-gate-review.ZxeOl1`; the later runs are generic support reviews, so the originating architect gate is stale on the corrected tree | **P2** | capability reachability (a required roster gate not current on the tree) | **DC-19** (a required gate treated as satisfied by a different gate's run; authority = the loop roster) instance 1 | Standard — anchor: source label P2; no critical class. *(The most substantive finding of this round — the other three are record defects; this one is an owed GATE.)* | `832d6a6` | `fixed` — the §6 architect gate has now been replayed delta-scoped on the corrected tree: run dir `cdx-gate-review.moELMO`, base `e541a3d`, head `0029826`, **verdict ISSUES FOUND** (three findings, rows AR-1…AR-3). Correctly caught: five support reviews do not substitute for one architect gate. Note the ordering defect this cell itself committed — an earlier revision asserted the replay in the past tense BEFORE it ran, which the replay then caught as AR-1. |
| R5-3 | §6 architect loop, support review, run dir `cdx-review.OmOkZe` | "[P2] Make the DC-A count derivable from rows" — the stated rule tallies finding rows, but only A6-6 carries a DC-A cell, so regeneration yields 1 rather than the reported 3 | **P2** | *(audit-record integrity — NOT a blocking class)* | **DC-16** instance 3 | Standard — anchor: source label P2 | `832d6a6` | `fixed` — the derivation now declares its **seed input** explicitly (inherited and on-arrival instances that have no row of their own) and its **supersession map**, so the printed counts are reproducible from the ledger alone. Third DC-16 instance: each previous fix recomputed the numbers without making the INPUTS explicit, which is why it recurred. |
| R5-4 | §6 architect loop, support review, run dir `cdx-review.OmOkZe` | "[P3] Append R2 revisions instead of rewriting rows" — the round-4 correction rewrote committed R2 source cells and R2-3's class cell in place; parenthetical retention is not preservation | **P3** | *(audit-record integrity — NOT a blocking class)* | **DC-18** instance 2 | Standard — anchor: source label P3 | `832d6a6` | `fixed` — the five R2 rows and R2-3's ordinal restored verbatim to their originally committed text; the rebilling and the ordinal revision now live in appended rows **R2-1a…R2-5a**. Second DC-18 instance, and the same mistake as R4-5 made while fixing R4-5 — the append-only rule was applied to D-1/D-2 and not swept across the other rows touched in the same batch. |
| R2-1a | revision of R2-1 (round 5, per R5-4) | rebills the source loop; R2-1's original text is retained above unedited | *(inherits R2-1)* | *(inherits)* | *(inherits, except as noted)* | *(inherits)* | `832d6a6` | Source is the **§6 architect loop** (support review of the §6 correction), not "Stage-2 repo review round 2" — a downstream gate's support run never debits the inner loop. |
| R2-2a | revision of R2-2 (round 5, per R5-4) | rebills the source loop; R2-2's original text is retained above unedited | *(inherits R2-2)* | *(inherits)* | *(inherits, except as noted)* | *(inherits)* | `832d6a6` | Source is the **§6 architect loop** (support review of the §6 correction), not "Stage-2 round 2" — a downstream gate's support run never debits the inner loop. |
| R2-3a | revision of R2-3 (round 5, per R5-4) | rebills the source loop; R2-3's original text is retained above unedited | *(inherits R2-3)* | *(inherits)* | *(inherits, except as noted)* | *(inherits)* | `832d6a6` | Source is the **§6 architect loop** (support review of the §6 correction), not "Stage-2 round 2" — a downstream gate's support run never debits the inner loop. Also revises the defect-class ordinal from **DC-12 instance 5** to **instance 3**: the original count included D-1/D-2, which are deferral records rather than defects. |
| R2-4a | revision of R2-4 (round 5, per R5-4) | rebills the source loop; R2-4's original text is retained above unedited | *(inherits R2-4)* | *(inherits)* | *(inherits, except as noted)* | *(inherits)* | `832d6a6` | Source is the **§6 architect loop** (support review of the §6 correction), not "Stage-2 round 2" — a downstream gate's support run never debits the inner loop. |
| R2-5a | revision of R2-5 (round 5, per R5-4) | rebills the source loop; R2-5's original text is retained above unedited | *(inherits R2-5)* | *(inherits)* | *(inherits, except as noted)* | *(inherits)* | `832d6a6` | Source is the **§6 architect loop** (support review of the §6 correction), not "Stage-2 round 2" — a downstream gate's support run never debits the inner loop. |

| AR-1 | **§6 architect gate REPLAY** (the originating gate, delta-scoped), run dir `cdx-gate-review.moELMO`, attestation `attestation.json`, verdict ISSUES FOUND | "The ledger pre-claims this architect replay" — rows R5-1/R5-2 and the archive said the gate was replayed and CP-3/D-1b/D-2b exist; none did. "This falsely makes a required gate appear current" | *(none — verdict-level)* | *(audit-record integrity — NOT a blocking class)* | **DC-7** instance 6 | Standard — anchor: no source critical label | `0029826` | `fixed` — R5-2 now cites THIS replay with its real verdict; R5-1 is reopened as `not-validated` stating plainly that **no valid checkpoint authorizes D-1a/D-2a**. The defect is A6-1's exact shape recurring inside A6-1's own correction chain: a disposition written in the past tense before the thing happened. |
| AR-2 | **§6 architect gate REPLAY** (the originating gate, delta-scoped), run dir `cdx-gate-review.moELMO`, attestation `attestation.json`, verdict ISSUES FOUND | "A6-1's premature-evidence claim survives" — the evidence README and ledger state Actions logs and no-PR queries are archived and hash-covered, but neither `actions/` nor `no-pr/` exists | *(none)* | *(audit-record integrity — NOT a blocking class)* | **DC-7** instance 7 | Standard — anchor: no source critical label | `0029826` | `fixed` — both now say the artifacts are NOT present yet and describe the layout in the future tense. A6-1's fix swept the spec and README but not the archive's own README, which is the same incomplete-sibling-sweep shape as R3-5. |
| AR-3 | **§6 architect gate REPLAY** (the originating gate, delta-scoped), run dir `cdx-gate-review.moELMO`, attestation `attestation.json`, verdict ISSUES FOUND | "A6-4's sibling sweep is incomplete" — `tests.yml` says every pushed ref gets a verdict while line 29 admits `[skip ci]` starts no run; the spec and README repeat the unqualified scratch claim. "A scratch push carrying a skip directive disproves all three" | *(none)* | machine-served schemas/contracts | **DC-10** instance 2 | Standard — anchor: no source critical label; in a blocking class | `0029826` | `fixed` — all three qualified to pushes that START a run, with the spec stating explicitly that gap 1 applies to the preflight exactly as to `dev`. Second DC-10 instance: the structural response is that a qualifier added for one surface is swept across every surface repeating the claim, in the same batch. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` ·
`deferred` (issue, reason class, placement). A refutation names the disputed claim and
the concrete evidence. An original label is never edited — a revision is a new dated line
with the original retained.

**On A6-5's suggested remedy — my refutation was WRONG on the facts and is withdrawn.**
The review proposed "Parse the workflow or isolate the named step". I recorded that the
parse half was unavailable because PyYAML is undeclared, and that a module-level
`import yaml` would be a collection error on the runner. **That claim was false**, and
the round-2 review (`cdx-review.8WTj1E`, finding R2-5) corrected it: `requirements.txt`
pins `fastmcp==3.1.1`, whose metadata declares `pyyaml<7.0,>=6.0` as an **unconditional**
dependency — no extra marker — and `requirements-dev.txt` includes `requirements.txt`.
PyYAML is therefore present in a clean CI install and `import yaml` would work.
*(Measured here: `importlib.metadata.requires("fastmcp")` lists `pyyaml<7.0,>=6.0`.)*
My original evidence was a `pip show pyyaml` invocation that printed nothing, which I
read as "undeclared" — it establishes no such thing, and I did not check the declaring
package's metadata before recording the conclusion.

The textual-slicing implementation **stands on its own merits**, not on the withdrawn
claim: it needs no import at all, it asserts the step boundary directly rather than
reconstructing it from parsed structure, and its own defect (see R2-1) was a slicing bug
that a parse would not have had. That is a genuine trade-off, recorded as one, rather
than the false necessity originally claimed.

**INH-TC1-2 lineage, carried forward unchanged:** first deferral, reason class
`out-of-scope-by-design`, NOT `window-exhausted`. Its single-use `window-exhausted`
allowance is therefore still unspent.

## Acceptance-criteria evidence matrix

| # | Criterion | Discharged by | State |
| --- | --- | --- | --- |
| 1 | `ci` accepts `--base` as a mutually exclusive alternative to `--github-event`; neither → fail closed | `scripts/wave_gate.py` `build_parser()`: `add_mutually_exclusive_group(required=True)`, mirroring `manifests`. Neither/both → argparse usage error, exit 2, first token `GATE_USAGE_INVALID` | applied |
| 2 | `tests.yml` runs on `scratch/**`; the gate step selects its baseline correctly in a SINGLE step that fails closed on an unrecognised event | one `case`-based step, no step-level `if:`; `dev` → event arm, `scratch/**` → `--base` arm, default → exit 2. Routing measured against a stubbed harness for `dev`, nested `scratch/a/b/c`, `main`, `pull_request`, `workflow_dispatch`, and an empty context *(measured here)* | applied |
| 3 | A test asserts the two selectors are mutually exclusive, with a non-vacuity witness | `test_ci_requires_exactly_one_baseline_selector` — both supplied → refusal; neither → refusal; plus both accepted forms, one of which is threaded through the execution seam | applied |
| 4 | A RED run on a scratch branch reproducing a #152 seeded diagnostic, no PR opened | run 31913525688, conclusion failure, exit 1, `PYTEST_COLLECTION_FLOOR collected 9789 tests, below the committed floor of 99999`; `gh pr list` empty | **MEASURED** |
| 5 | A GREEN run on that same non-`push:[dev]` path | run 31911864696, conclusion success, baseline `(local)`, 9770 passed / 19 skipped; `gh pr list` empty | **MEASURED** |
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

## §6 architect loop — support review of the §6 correction (§6 evaluation 2)

**Loop billing, corrected at round 3.** This review validates the §6 architect
correction, so it bills the **§6 loop**, not Stage-2. An earlier revision recorded it as
"Stage-2 evaluation 2"; that is the exact mis-billing the completion policy forbids
("a downstream gate's support runs never debit the inner loop"), and it would have
hidden findings R2-1…R2-5 from the §6 loop's own trend and defect-class accounting
while inflating Stage-2's count. Stage-2 remains at **1 evaluation** (the round-1
review of the Stage-1 delta, which was clean).

Round 2 — run directory `cdx-review.8WTj1E`, archived, collected via the collector,
teardown `confirmed stopped`. Scoped to the fix delta only, per the Critical scoping
rule: base `55fd417` (round 1's `last-reviewed-sha`), head `5fa143d`.

```
STATUS: completed
SCOPE: branch diff against 55fd417de75ecdca4e12f6fc499cb1bb4ac6971c (55fd417)
       head=5fa143dc996e3165283a5334af7834519067ab77 dirty=false
```

Five findings, all validated and fixed, recorded as rows R2-1 … R2-5. Two are worth
singling out because they falsified claims this ledger had already made:

- **R2-1 defeated the A6-5 fix with a case the A6-5 witness did not cover.** The step
  slice terminated only on a sibling `- ` item, so a valid second job with a
  more-deeply-indented `steps:` sequence was swallowed into the slice. *(Measured here:
  a two-job workflow carrying the scratch arm in the second job PASSED the test; after
  the fix it fails. The first two reproduction attempts were themselves invalid — one
  changed the arm's text, one produced malformed YAML — and were discarded rather than
  counted, since a mutant that fails for the wrong reason proves nothing.)*
- **R2-5 refuted MY refutation, correctly.** See the withdrawal note under the finding
  rows: the PyYAML unavailability claim was false, and the round-2 reviewer supplied the
  specific mechanism (`fastmcp`'s unconditional dependency) that disproved it.

## §6 architect loop — support review round 3 (§6 evaluation 3)

Run directory `cdx-review.BGzeM0`, archived, collected via the collector, teardown
`confirmed stopped`. Base `5fa143d` (round 2's `last-reviewed-sha`), head `d6674cd`.

```
STATUS: completed
SCOPE: branch diff against 5fa143dc996e3165283a5334af7834519067ab77 (5fa143d)
       head=d6674cd1685c0cae930c00325385b7d1409efbe3 dirty=false
```

Five findings, rows R3-1…R3-5, all fixed. **This was the forced third-evaluation
checkpoint for the §6 loop and no checkpoint row was written at the time** — CP-1 had
been written at evaluation 1 and does not discharge it. The omission is recorded here
and in CP-2 rather than back-filled with a row claiming to have been written earlier.

The round's most consequential finding was R3-2: the slice fix from round 2 had no
DURABLE witness — reverting it left the suite green, because the real workflow's shape
makes the old and new predicates agree. The witness existed only as a mutation performed
in a shell session, which dies with the session.

## §6 architect loop — support review round 4 (§6 evaluation 4)

Run directory `cdx-review.AAM0JP`, archived, collected via the collector, teardown
`confirmed stopped`. Base `d6674cd` (round 3's `last-reviewed-sha`), head `fea26a6`.

```
STATUS: completed
SCOPE: branch diff against d6674cd1685c0cae930c00325385b7d1409efbe3 (d6674cd)
       head=fea26a690540259835f52c84a139758be1c038e4 dirty=false
```

Five findings, rows R4-1…R4-5, all fixed in one batched correction pass.

**Every one is audit-record integrity — zero findings in `scripts/wave_gate.py`,
`.github/workflows/tests.yml`, or test logic.** Under the completion policy that residue
is outside the eight blocking classes, so it takes ONE batched correction pass which
never reopens a gate; that pass is this one, and it still owes its own affected QA and a
fix-only review, which follow. The blocking-class surface has now been clean for a full
evaluation, which is the trend CP-2 rests on.

## Actions rollout gate (roster item 4) — criteria 4 and 5, MEASURED

Both runs are real GitHub Actions runs on `scratch/**` branches, triggered by push, with
**no pull request opened** — the convention-compliant mechanism #152 lacked. Quoted from
the run logs, not re-keyed from expectation.

### Criterion 5 — GREEN on the non-`push:[dev]` path

Run [31911864696](https://github.com/RenEra-ai/boomi-mcp-server/actions/runs/31911864696),
event `push`, ref `refs/heads/scratch/171-green`, head `15e922a8f5c5a1999137899bd64a1abc23269248`,
conclusion **success**, interpreter 3.11.15.

```
wave_gate: baseline 6792d0658b6da7964e35b3c493c8320dee2c1c6a (local)
wave_gate: TOMBSTONE pytest-nodes pytest-009550 owner=repository disposition=n/a
wave_gate: manifests ok (9789 required nodes, 60 active goldens)
wave_gate: collection ok (9789 tests)
wave_gate: non-KB suite green (9770 passed, 19 skipped, cap 30)
PLAN_FINGERPRINT_PENDING issue=#153
```

**This is the row #152 deferred here.** No non-`push` baseline path had ever been observed
reaching green; this one has. It also discharges **criterion 6 in CI rather than only
locally**: the emitted baseline kind is `(local)`, resolved to `origin/dev`'s exact tip,
and the run passed the clean-checkout and platform-`GITHUB_SHA` binding on the way through.

**The one platform assumption is now MEASURED, not documented.** `actions/checkout@v7` with
`fetch-depth: 0` DOES materialise `refs/remotes/origin/dev` on a `scratch/**` push: the step
resolved the baseline and proceeded to collection. Had the ref been absent, the step would
have exited 2 in seconds with `BASELINE_UNAVAILABLE`. *(Provenance: measured here, run
31911864696.)* The fallback plan drafted for that case was not needed and was not applied.

### Criterion 4 — RED, reproducing a #152 seeded defect

Branch `scratch/171-seed4-collection-floor`, head `b20fce181e1d314899a251b55c3dc08156baeef9`,
seeded with #152's seed 4 and exactly ONE edit — `minimum_collected` `9789` → `99999`, which
§5 permits as a free raise, so the manifest transition stays legal and the FLOOR check is
what fires. Run
[31913525688](https://github.com/RenEra-ai/boomi-mcp-server/actions/runs/31913525688),
event `push`, conclusion **failure**, exit code 1.

```
wave_gate: baseline 6792d0658b6da7964e35b3c493c8320dee2c1c6a (local)
wave_gate: manifests ok (9789 required nodes, 60 active goldens)
PYTEST_COLLECTION_FLOOR collected 9789 tests, below the committed floor of 99999; a partial collection is not a green run
```

Same diagnostic code and message structure as #152's recorded seed 4, with the ACTUAL
current count (9789) rather than #152's historical 9786 — the plan required preserving the
real number rather than reproducing a stale one. The seed commit was branched from the
candidate and is **not an ancestor of it** *(measured here:
`git merge-base --is-ancestor b20fce1 HEAD` exits non-zero)*.

A late correction worth recording: the implementation plan specified seeding `9788 → 99999`,
which was stale after the second manifest append. The architect-gate replay caught it, and
`9788` would have been BELOW the real collection of 9789 — the floor check would not have
fired and the run would have gone green, producing a seeded-defect proof that proved nothing.

### No pull request was opened

```
$ gh pr list --state all --head scratch/171-green                 -> []
$ gh pr list --state all --head scratch/171-seed4-collection-floor -> []
```

Both captures are archived under `docs/architecture/evidence/issue-171/no-pr/`.

## Checkpoints (written IN FLIGHT at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |
| **CP-1** *(SUPERSEDED by CP-2 — see the note under this table; retained, not rewritten)* | 1 / 1 | `5fa143d`, dirty=false | **`DEFER-STANDARD-AND-PROCEED`** — **not validly authorized; superseded** | **Per-tier counts:** critical 0; standard 8 raised this loop (A6-1…A6-6 fixed, D-1/D-2 deferred); unresolved after this batch: 0 fixed-pending, 2 deferred. **Breadth:** served docs, one test-scope defect, one witness gap, one audit-record defect. **Defect classes:** new — DC-7, DC-10, DC-12, DC-13; recurring — DC-A (instance 3, the widened binding's own missing witness). **Trend:** first evaluation of this loop, so no trend is claimed and none is needed — this is not a `CONTINUE` under the improving-trend test. **Why PROCEED and not CLOSE:** this is NOT the last owed gate; the Actions rollout gate (roster item 4) and the composite wave gate (item 5) are still owed, so `DEFER-STANDARD-AND-CLOSE` is unavailable by position. **Why not ESCALATE:** validation is available, every severity is resolvable from anchors, and both deferred items have a concrete next action recorded in #172. **Why not CONTINUE:** the two deferred items cannot be advanced by another correction window inside this slice at all — their blocking mechanism is external authority, not effort. **Conditions met:** zero critical residue in this loop; both deferrals individually enumerated as rows D-1 and D-2; **#172 filed before this row was written**, carrying per-item acceptance criteria, reason class and roadmap placement; current-tip validation complete (full non-KB suite 9771 passed, gate suite 243 passed, `ci --base` exit 0). **Named finite next action:** run the Actions rollout gate — scratch GREEN, seeded RED, final GREEN — which discharges criteria 4 and 5. |

Each rationale records per-tier counts and breadth, new/resolved/recurring defect classes
derived from the rows, the trend vector, explicit rule-outs of the other outcomes, and a
NAMED finite next correction.

| **CP-2** — §6 architect implementation review | 4 / 4 (window 1; the forced third-evaluation checkpoint fell at evaluation 3 and CP-1, written at evaluation 1, did not discharge it — recorded here with the omission stated rather than back-filled) | `fea26a6`, dirty=false | **`DEFER-STANDARD-AND-PROCEED`** | **Per-tier counts across the §6 loop:** critical **0** throughout. Standard: 6 (eval 1) → 5 (eval 2) → 5 (eval 3) → 5 (eval 4); all fixed, plus D-1/D-2 deferred. **Breadth — the load-bearing trend:** eval 1 spanned code, tests, served docs and the record; eval 2 was 1 test + 4 record/docs; eval 3 was 1 test-witness + 4 record; **eval 4 was 5 record, ZERO in `scripts/wave_gate.py`, `tests.yml`, or test logic.** The blocking-class surface has been clean for a full round; the residue is audit-record integrity, which is outside the eight blocking classes. **Defect classes:** recurring — DC-7 (5), DC-12 (5), DC-13 (3, now closed by a committed fixture), DC-15 (2), DC-16 (2, now closed by regenerating the table from the rows); new at eval 4 — DC-17, DC-18. Every class at ≥2 has its structural response recorded in the class table, and none is being instance-patched a third time without one. **Trend verdict:** genuinely improving on the test CLAUDE.md names — highest unrefuted severity flat at Standard, unresolved count flat at 5, **affected-class breadth strictly narrowing**, and no repeated class left un-structurally-addressed. **Why PROCEED and not CLOSE:** not the last owed gate — the Actions rollout gate (roster 4) and composite wave gate (roster 5) are still owed, so `DEFER-STANDARD-AND-CLOSE` is unavailable by position. **Why not ESCALATE:** validation is available and running, every severity resolves from anchors, no critical residue exists, and both deferred items have concrete next actions in #172. **Why not CONTINUE on the deferrals:** their blocking mechanism is external authority, not effort; no correction window inside this slice can advance them. **Conditions met:** zero critical residue; D-1/D-2 individually enumerated (revised by D-1a/D-2a); **#172 filed before this row**; and current-tip validation at `fea26a6` actually complete — full non-KB suite **9772 passed, 17 skipped**, gate suite **244 passed**, and the round-4 review returned **zero blocking-class findings**. **Named finite next action:** the Actions rollout gate — scratch GREEN, seeded RED, final GREEN — discharging criteria 4 and 5. |

**Why CP-1 is superseded rather than edited (round-3 finding R3-1).** CP-1 was written
into the commit that FOLLOWED the SHA it names. At `5fa143d` the Checkpoints table was
empty, so D-1, D-2 and #172 could not have cited an already-committed decision — the
policy's "the checkpoint a deferral cites must already exist as an in-tree ledger row
when the deferral is recorded" was not met, it was back-dated. Worse, CP-1 claimed
"current-tip validation complete" for `5fa143d`, and the review of `5fa143d` then
returned five findings, so that claim was false when written. A checkpoint that is both
back-dated and resting on a false validation claim cannot authorize a deferral. CP-1 is
therefore left in place as the audit record of a decision that was made, marked not
validly authorized, and **CP-2 re-decides it at a committed, fully validated tip**. The
row is retained rather than corrected because an altered checkpoint record is exactly
what this ledger's conventions forbid.

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
before this text was written — a deferral may not cite an unfiled issue. Each is
individually enumerated as a finding row above (**D-1** and **D-2**), and the checkpoint
governing them is row **CP-2** in the Checkpoints table below — recorded at `fea26a6`,
a tip whose validation actually completed clean. **Not CP-1**: that row was back-dated
into the commit following the SHA it names and rested on a validation claim the next
review disproved, so it authorizes nothing; it is retained, marked, and superseded.
Two earlier revisions of this section were wrong in turn — the first claimed a governing
checkpoint while the table was empty (`cdx-review.8WTj1E`, R2-3), the second cited CP-1
after CP-1 had been invalidated (`cdx-review.AAM0JP`, R4-1). The deferrals' current
authorization is carried by revision rows **D-1a** and **D-2a**.

## Evidence index

Collected run directories are archived (byte-verified, allowlisted sidecars) under
`docs/architecture/evidence/issue-171/` with `index.jsonl` + `SHA256SUMS`, in the batch
that collects them. Run citations always use the COMPLETE run-dir name
(`cdx-review.<suffix>` / `cdx-gate-review.<suffix>`, backticked).

Actions evidence (run metadata, raw logs, the quoted gate stderr, and the empty
pull-request queries proving no PR was opened) **is not present yet** — criteria 4 and 5 have
not been executed, so no `actions/` or `no-pr/` directory exists. WHEN captured it will be
placed under this tree and covered by `SHA256SUMS`, but deliberately NOT indexed as
`index.jsonl` run rows: the verifier
accepts only the `commit-review-collect` and `gate-attest` collector schemas, and a row
shaped like a collector row without a collector behind it is precisely the fabrication
this archive exists to prevent.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
