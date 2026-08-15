# Issue #152 — audit record (in progress)

Baseline (Stage 1 step 0): `9080e3c2d0fcc82b01f781b2352d60995ba58ad8`
Branch: `codex/issue-152`. Slice class: **dark** (zero `src/` change).

## Loop roster (enumerated in advance, before the first correction)

1. Stage-1 QA — darkness proof + gate validation (this slice's QA is the darkness
   proof per CLAUDE.md: tests/docs-only).
2. Stage-2 Codex repo review (`--base 9080e3c`), plus delta-scoped re-reviews.
3. §6 architect implementation review (the wrapping /codex-issue pipeline's
   additive gate), plus delta-scoped re-reviews.
4. Terminal correction loop, if one is needed.

No wave gate is owed separately: this slice IS the wave-gate implementation, and
its own `wave` command was run at the tip (see evidence).

## Findings found and fixed BEFORE any gate (self-review + measurement)

These are not gate findings; they are recorded because they changed the tree.

| # | Finding | Class | How found | Disposition |
|---|---|---|---|---|
| S1 | Non-KB suite RED on Python 3.11 (the production interpreter): 5 × `test_recipe_security.py` — pydantic refuses `typing.TypedDict` on <3.12 (`PydanticUserError typed-dict-version`) | runtime behavior (test harness) | measured, first 3.11 run | fixed (spelling alias + one skipif with reason) |
| S2 | `test_recipe_registry.py::test_non_schema_keys_...` — `typing.is_typeddict` is False for every `pydantic_core` TypedDict on <3.12, so the tripwire derived ZERO and asserted vacuously-then-failed | runtime behavior (test harness) | measured | fixed (`typing_extensions.is_typeddict`, both sites) |
| S3 | Same defect class at a SECOND site (`bears_schema`) — recursion stopped at the first nested TypedDict, reporting `fields` as uncovered | runtime behavior | measured after S2 | fixed at both consumers (structural-fix rule) |
| S4 | `test_m12_migration_matrix_evidence._collected_ids` collected `tests/kb/`, replaced `PATH` with a macOS-only literal, wrote bytecode | runtime behavior | read | fixed + argv/env pinned by a new test |
| S5 | `scripts/wave_gate.py` moves the #149 freeze's `python_source_count` (scripts/ is in the scan universe) | mutation accounting (baseline artifact) | measured | fixed (205→206, the deliberate reviewed bump the freeze is designed for) |
| S6 | Gate's node-line regex dropped 177 parametrized ids containing spaces | machine-served contract | caught by the gate's OWN summary-reconciliation check | fixed |
| S7 | `_check_uniqueness` required uniqueness on every immutable field, making the real 60-row manifest unparseable (`renderer` repeats) | machine-served contract | caught running the real manifest | fixed (split `payload` vs `unique`) |
| S8 | **Bootstrap unreachable in CI** — `check_bootstrap` demanded `--bootstrap`, which `ci` has no flag for; the run that lands the manifests could never go green | capability reachability | self-review of source | fixed (flag required only for local; CI's event baseline is the evidence) + 2 tests |
| S9 | Gate ran pytest with a 4-entry `PYTHONPATH`, but the manifest was captured under the documented `PYTHONPATH=src` | machine-served contract | self-review | fixed (split pytest env vs render env) |
| S10 | `execute()` leaked its `mkdtemp` on every run | hygiene (non-blocking) | self-review | fixed (try/finally) |

## Deviations from the architect design plan (each evidenced)

| # | Architect said | Shipped | Why |
|---|---|---|---|
| D1 | Refactor 13 golden-producing test modules onto a shared corpus | Not done. Registry IMPORTS their existing module-level helpers | Measured import cost 0.39 s for all 13 (planner assumed ~4.9 s). Zero duplication AND zero edits to high-value regression suites — strictly better than both the architect's refactor and the planner's duplication. |
| D2 | `try_catch_dlq_error_subprocess.xml` = `deletion_only` / #160 | `survivor`. No `deletion_only` row committed | `capability-parity-backlog.md:97` lists it as working today, and `139-criterion-audit.md:89` plans its canonical successor. The evidenced pair is the two `*_archetype.xml` (`:146` "retire with the archetype") → `transitional_oracle` / #159. `survivor` is the only value correctable later without an illegal edit. |
| D3 | Golden test parametrized over ACTIVE rows | Over ALL rows, keyed by immutable manifest id | Active-only couples the ledgers: a golden tombstone would delete a pytest node id and trip `PYTEST_NODE_MISSING`, forcing #159/#160 into a pointless paired edit. (planner's correction, adopted) |
| D4 | Local bootstrap needs `HEAD == bootstrap_base` or a single introduction commit | `--bootstrap` + `--base` == declared base + ancestry proof | The architect's form is unsatisfiable on a working branch with several commits, making the gate unrunnable on the very change it bootstraps. Ancestry is what forbids re-bootstrap. (planner's correction, adopted) |
| D5 | `PYTEST_DISABLE_PLUGIN_AUTOLOAD: "1"` in CI | dropped | `anyio` DOES register a pytest11 plugin; the flag would have changed the configuration I validated. |
| D6 | CI env sets `BOOMI_LOCAL`/`BOOMI_DOCS_ENABLED`/`BOOMI_GOTCHAS_ENABLED` | not set | The suite is green with a plain environment (measured). Setting unvalidated vars changes behaviour away from what was tested. |
| D7 | `minimum_active` only | + `minimum_collected` | The issue requires the collection floor to trip INDEPENDENTLY of the required-node check; with one number the floor is subsumed and cannot be demonstrated alone. |

## Loop 1 — Stage-1 QA (boomi-qa-tester), evaluation 1

Report: `agents/reports/2026-08-13-issue-152-ci-wave-gate-r1.md` (gitignored — repo convention).
Verdict: darkness holds; gate failed closed on 36/36 seeded defects; 4 findings, 0 critical.

| Source ID | Verbatim summary | Source label | Blocking class | Derived tier + anchor | Disposition |
|---|---|---|---|---|---|
| QA-152-r1-01 | "The ledger pins a test's *existence*, never its *execution*… one `pytestmark = pytest.mark.skip` neutralised **119** required tests with every gate phase green and no ledger edit" | Medium | capability reachability | **Standard** (blocking class, not secrets/data-loss/mutation-accounting, source label not P0/P1/High) | **fixed** |
| QA-152-r1-02 | "`requirements-dev.txt` calls itself 'the complete, authoritative dependency set' while `typing_extensions` … is declared nowhere and arrives only transitively via pydantic" | Low | none (packaging prose + real dep) | Non-blocking | **fixed** |
| QA-152-r1-03 | "`check_worktree_unchanged` is blind to three change classes the README says it catches"; module docstring accurate, README paraphrase over-claims | Low | none (README prose) | Non-blocking | **fixed** (README reworded to state exactly what is compared) |
| QA-152-r1-04 | "`gh api .../rulesets` returns `[]`: nothing requires `Python 3.11 non-KB` yet" | Low / operational | none (post-merge operator action) | Non-blocking | **not-validated → carried to the final report as an owner action.** Not a code defect: the workflow cannot configure its own branch protection, and §10 of the gate spec already documents it as manual. I did not change the repo's ruleset — that is an outward-facing repo-settings change and it is the owner's call. |

**QA-152-r1-01 verification (I checked before implementing, per `receiving-code-review`):**
- `test_build_context_completeness.py:46` really is `@pytest.mark.skipif(shutil.which("gcloud") is None, …)`, its own docstring says "Skipped when `gcloud` is unavailable (e.g. bare CI)", and it really is an active required row (1 match in the node manifest).
- Independent probe: a 2-test module with `pytestmark = pytest.mark.skip` → `2 tests collected`, `2 skipped`, **rc=0**. Collection, floor, required-node and exit code all stay green.
- Finding CONFIRMED. Fixed in the slice's own idiom rather than deferred, because the QA agent's timing argument is correct: header fields are part of the manifest schema, and adding one after the ledger lands would require a `schema_version` bump, which `validate_transition` refuses as an immutable-header change. It was fix-now or design a migration.

Fix shipped: `maximum_skipped` header field (30); `run_suite` now parses pytest's
outcome summary and asserts `skipped <= cap`, `passed >= collected - cap`, and
`failed == errors == 0` even on a zero exit; unparseable summary is
`PYTEST_SUMMARY_UNPARSEABLE`; the cap may be lowered but **never raised** by a
transition. 8 new tests; +3 diagnostic codes, all documented (enforced by
`test_every_diagnostic_code_the_gate_can_raise_is_documented`).

## Loop 2 — Stage-2 Codex repo review, evaluation 1

Run dir `/tmp/cdx-review.pGkjvF`. Attestation: `STATUS: completed`,
`SCOPE: branch diff against 9080e3c2d0fcc82b01f781b2352d60995ba58ad8 (9080e3c) head=3a6f65b dirty=false`.
6 findings, **0 false positives** — every one verified against the code before any fix.

| Source ID | Verbatim summary (abridged) | Source label | Blocking class | Derived tier + anchor | Disposition |
|---|---|---|---|---|---|
| CDX-r1-01 | "Bind bootstrap to the first manifest introduction… `check_bootstrap` returns true again and `run_manifest_phase` skips every `validate_transition` call, allowing a rewritten ledger to pass" | **P1** | machine-served contract / mutation accounting | **Critical** (source label P1) | **fixed** |
| CDX-r1-02 | "Reject tombstones whose pytest nodes still collect… both runs pass because the floor reduction was prepaid" | **P1** | capability reachability | **Critical** (source label P1) | **fixed** |
| CDX-r1-03 | "Check relocation for mutated fingerprints… every current assertion passes even though those resulting plans are not relocatable" | P2 | machine-served contract (#153 seam) | Standard | **fixed** |
| CDX-r1-04 | "Run worktree hygiene checks after failures… control never reaches `check_worktree_unchanged`" | P2 | mutation accounting | Standard | **fixed** |
| CDX-r1-05 | "Isolate push runs from the shared concurrency queue… the middle commit's run is canceled without a completed test verdict" | P2 | capability reachability | Standard | **fixed** |
| CDX-r1-06 | "Reject nested content in the golden directory… the gate remains green despite claiming set equality" | P2 | machine-served contract | Standard | **fixed** |

**Verification performed before fixing (per `receiving-code-review`):**
- CDX-r1-01 reproduced end-to-end on the real repo: with the manifests committed at
  `3a6f65b`, `manifests --base 9080e3c --bootstrap` on a ledger whose immutable
  `owner` field had been mutated on an existing row exited **0**. Confirmed bypass.
- CDX-r1-02 reproduced by direct call: a tombstoned row whose node is still
  collected was **accepted**, while the golden side already enforced the mirror
  rule (`GOLDEN_FILE_UNDECLARED`). Confirmed asymmetry.
- CDX-r1-03/04/06 confirmed by reading the code paths named.
- CDX-r1-05 matches GitHub's documented concurrency behaviour (a new run cancels a
  previously *pending* run in the group regardless of `cancel-in-progress`).

**One reviewer-suggested mechanism rejected on technical grounds, and something
better substituted.** CDX-r1-01 said "Prove that the current delta is the first
introduction rather than checking only the baseline's ancestors." I first
implemented that literally as a commit-count rule (≤1 commit in `baseline..HEAD`
may touch a manifest) and then removed it: it refuses ordinary multi-commit
development of the very change that introduces the ledger — this slice could not
have validated itself after its own second commit. The defect the reviewer
identified is real; the discriminator is not *how many commits touched the file*
but *whether the ledger has landed*, so the fix carries the PR's `base.sha`
alongside the merge base and refuses bootstrap when the manifests already exist
on the target. That also closes the reviewer's second case ("a PR that retains
that merge base"), which the commit-count rule did **not** close: GitHub checks
out `refs/pull/N/merge`, so the tree has the manifests while `head.sha` stays
un-merged and the merge base stays stale forever. Both the rejected rule and the
residual local-run gap are recorded in the gate spec so neither is silently
reintroduced.

## Loop 2 — Stage-2 Codex repo review, evaluation 2 (delta-scoped)

Run dir `/tmp/cdx-review.qY0XP8`. Attestation: `STATUS: completed`,
`SCOPE: branch diff against 3a6f65b1c79f5616eef75670810d08bd09186a8c (3a6f65b) head=5f11513a5784b7c0fc409709f08ecc1eb4bee776 dirty=false`.
2 findings, **0 false positives**. Both were defects I introduced in round 1 —
the review round did exactly what it exists for.

| Source ID | Verbatim summary (abridged) | Source label | Blocking class | Derived tier | Disposition |
|---|---|---|---|---|---|
| CDX-r2-01 | "Restrict the target probe to bootstrap candidates… every such PR fails until rebased despite being a normal transition" | **P1** | capability reachability | **Critical** | **fixed** |
| CDX-r2-02 | "Append new tests without repointing existing rows… this insertion repoints that immutable ID and shifts every later row" | **P1** | machine-served contract / mutation accounting | **Critical** | **fixed** |

**CDX-r2-01 verified:** the target probe ran ahead of the `all(present) → return
False` early exit, so it fired on any PR whose target had advanced — i.e. every
PR once the ledger has landed, none of which is claiming a bootstrap. Severe
false-positive that I introduced while fixing CDX-r1-01. Fixed by deciding
*whether a bootstrap is claimed at all* first and probing the target only then.

**CDX-r2-02 verified by measurement, and it is the more important of the two.**
Diffing the committed manifests at `3a6f65b` and `5f11513`: **321 rows had their
immutable `node_id` repointed**, 7 added, 0 removed. Root cause was not the
edit but the PROCEDURE: regenerating by "sort all collected node ids, number
from 1" shifts every row whose alphabetical position moved. The documented §9
procedure said exactly that, so every future slice would have produced an
illegal transition — the append-only contract would have been unusable in
practice. Fixed at the procedure level: the generator now preserves each
existing row's id/node_id/position and appends only new nodes (regardless of
where they sort), refuses to silently drop a node that stopped collecting, and
§9 documents this with the measurement. Regenerated append-only: 9,672
preserved + 9 appended = 9,681.

**Validation gap this exposed, now closed:** every local run I had made used the
BOOTSTRAP arm (`--base 9080e3c --bootstrap`), which skips transition checking
entirely — so no local run could ever have caught a bad transition. The
intra-branch arm is now part of the procedure and was run:
`manifests --base 3a6f65b` → exit 0.

## Loop 3 — §6 architect implementation review (additive gate), evaluation 1

Run dir `/tmp/cdx-gate-review.WEvspW`. Attestation `ok:true`, gate `review`,
`parsedVerdict: ISSUES FOUND`, prompt+plan both verified inside the attested
prompt. 8 findings + a rollout observation. No severity labels from this gate, so
tiers derive from blocking class.

| Source ID | Verbatim summary (abridged) | Blocking class | Tier | Disposition |
|---|---|---|---|---|
| ARCH-01 | "PR CI can validate an unrelated checkout and pass" — event head/target never bound to the actual `HEAD` | machine-served contract | Standard | **fixed** (`check_checkout_matches_event`, new code `CHECKOUT_EVENT_MISMATCH`; accepts a bare head or a `refs/pull/N/merge` checkout) |
| ARCH-02a | "PR bootstrap does not require `pull_request.base.sha == bootstrap_base`" → green PR, red merge | machine-served contract | Standard | **fixed** |
| ARCH-02b | "Local bootstrap has no required relationship between `HEAD` and `bootstrap_base`… calling local `wave` 'advisory' does not justify weakening it" | capability reachability | Standard | **fixed** — see refutation note below |
| ARCH-03 | "Ambiguous local baselines are accepted" (`git rev-parse` ref precedence) | machine-served contract | Standard | **fixed** (`_refuse_ambiguous`) |
| ARCH-04 | "The #153 seam can pass without testing a real canonical fingerprint" | machine-served contract | Standard | **fixed** — provider now returns `(digest, canonical_material)`, all four mutation kinds required, material compared across identities, provider exceptions kept on the coded path |
| ARCH-05 | "Ordinary CI does not prove every active golden ran" — a renderer calling `pytest.skip()` stays green inside the cap | capability reachability | Standard | **fixed** in the corpus, which closes both the per-commit and per-wave paths |
| ARCH-06a | "`active_count > minimum_active` accepted; the plan required equality… bootstrap can establish a permanently weakened floor" | machine-served contract | Standard | **fixed** (`!=`) |
| ARCH-06b | "Legal tombstones are counted… then the result is discarded"; owner/disposition never reported | mutation accounting | Standard | **fixed** (per-transition `TOMBSTONE` line with immutable owner + disposition) |
| ARCH-07 | env vars omitted; `wave --base` handled by argparse with no code; `GOLDEN_INPUT_MISSING`/`GOLDEN_FLOOR` absent; several negatives missing | mixed | Standard / non-blocking | **split** — argparse→coded fixed; missing negatives added (multiple merge bases, checkout mismatch, invalid UTF-8, symlinked manifest); env vars and the two alias codes **refuted, with the rationale moved into the shipped spec §9a** |
| ARCH-08 | "The worktree check does not prove byte preservation" | mutation accounting | Standard | **partially fixed** — `_status` now folds in `HEAD` and `git diff --numstat` (staged and unstaged), so an in-place edit to an already-dirty file, a commit, or a checkout is caught; a same-line-count edit still is not, and the README states exactly what is compared |
| ARCH-final | "rollout acceptance is not yet evidenced… despite the 'shipped' and ruleset claims" | machine-served contract (served docs) | **Standard, and the most embarrassing** | **fixed** — the doc said "Status: shipped" and the README said the ruleset "requires it"; neither was true. Both now state the actual position and name the owner rollout steps |

**Refutations, with reasoning (not compliance theatre):**

* **ARCH-07 env vars.** `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` would DISABLE `anyio`'s
  `pytest11` plugin, which two modules depend on — the flag would change the
  configuration that was validated rather than lock it in. `BOOMI_LOCAL` /
  `BOOMI_DOCS_ENABLED` / `BOOMI_GOTCHAS_ENABLED` are real variables, but the
  suite is green with none of them set; shipping values no validating run used
  would be a guess. The legitimate half of the finding — that this was decided
  nowhere a reader could see it — is fixed: spec §9a now records it.
* **ARCH-07 alias codes.** `GOLDEN_INPUT_MISSING` and `GOLDEN_FLOOR` would add no
  check: a missing case input already surfaces as `GOLDEN_RENDER_FAILED`, the
  golden floor as `MANIFEST_FLOOR_INVALID`. Minting aliases lengthens the roster
  without tightening the gate.
* **ARCH-02b mechanism.** The reviewer's framing implies a `HEAD`↔`bootstrap_base`
  relationship. Implemented literally (either as "≤1 commit touching the ledger"
  or "ledger unchanged since its introducing commit"), it rejects ordinary
  multi-commit development of the change that introduces the ledger — my own
  round-2 regression test caught the second form doing exactly that. The
  discriminator that separates "still developing" from "already landed" is
  REACHABILITY: the introducing commit is contained in some branch other than the
  one being worked on. That closes the reviewer's actual concern without the
  false positive, and both rejected mechanisms are recorded in the spec so
  neither gets reintroduced.
* **Disposition mapping.** The reviewer asks for the 57/2/1 mapping to be
  "restored or explicitly amended". It is already explicitly amended, in shipped
  §3, with the evidence and the immutability argument — not silently diverged.

## Loop 3 — evaluation 2 (support repo Codex review of the §6 correction delta)

Run dir `/tmp/cdx-review.4EYPwa`. Attestation: `STATUS: completed`,
`SCOPE: branch diff against f4235b9 (f4235b9) head=6111f895 dirty=false`.
7 findings, **0 false positives** — every one against code added in the previous
round. Per CLAUDE.md, support-validation findings stay in the owning loop.

| Source ID | Verbatim summary (abridged) | Blocking class | Tier | Disposition |
|---|---|---|---|---|
| SUP-01 | "Ignore mirrors of the current bootstrap branch… makes the required local wave gate unusable after the first push" | capability reachability | **Critical** (P1) | **fixed** — mirror filter (`ref != current and not ref.endswith('/'+current)`); detached HEAD now refuses rather than guesses |
| SUP-02 | "Require a clean tree for event-bound validation… allowing the gate to validate bytes not present in the push or PR" | machine-served contract | **Critical** (P1) | **fixed** — event contexts require a clean tree; local `--base` keeps dirty support |
| SUP-03 | "Reject push events without an after SHA… defeating the new event binding" | machine-served contract | Standard | **fixed** — `after` validated as strictly as `before` |
| SUP-04 | "Convert pytest xfail into a renderer failure" — `XFailed` subclasses `Failed`, exact-name match missed it | capability reachability | Standard | **fixed** — match the whole MRO |
| SUP-05 | "Materialize provider iterables inside the wrapper" — `list()` outside the guard | machine-served contract | Standard | **fixed** — `_provider_strings` materialises and type-checks inside the guard |
| SUP-06 | "Verify the digest against canonical material" — the two members can drift | machine-served contract | Standard | **fixed** — digest must equal `sha256:` + SHA-256 of the material, recomputed |
| SUP-07 | "Verify mutation kinds produce distinct plans" | machine-served contract | Standard | **fixed** — the four kinds must yield pairwise-distinct material |

Verified before fixing: SUP-01 by enumerating `git for-each-ref --contains` (a
pushed `origin/<branch>` mirror is not equal to `<branch>` and would read as
landed); SUP-04 by printing `XFailed.__mro__` (`XFailed → Failed → OutcomeException`),
confirming the exact-name test misses it.

**Loop 3 is at evaluation 2. Evaluation 3 forces a recorded checkpoint decision.**

## Loop 3 — evaluation 3 (support repo review) + CHECKPOINT

Run dir `/tmp/cdx-review.BzhRqS`. Attestation: `STATUS: completed`,
`SCOPE: branch diff against 6111f895 (6111f89) head=ff8e3100 dirty=false`.
**1 finding**, P1, 0 false positives.

| Source ID | Verbatim summary (abridged) | Blocking class | Tier | Disposition |
|---|---|---|---|---|
| SUP2-01 | "Do not exempt a landed `origin/dev` ref… when the current branch is `dev`… this predicate discards both… allowing immutable row rewrites to pass" | machine-served contract / mutation accounting | **Critical** (P1) | **fixed** |

Verified by construction before fixing: with `current="dev"` and containing refs
`["dev", "origin/dev"]`, the mirror predicate yields `landed_on == []` → bootstrap
re-claimable. This repo lands by fast-forward push to `dev`, so it is the
realistic case, not a corner.

**Partial pushback recorded:** this reverses the mirror exemption I added for
SUP-01 last round. SUP-01's premise — that the strict rule makes "the required
local wave gate unusable after the first push" — is overstated: only the
BOOTSTRAP ARM becomes unavailable; `wave --base <a commit carrying the
manifests>` still runs the transition, the full suite and every golden, and is
the more appropriate check once the ledger is committed. Trading a real bypass on
the integration branch for that convenience was the wrong trade, and both the
attempt and the reversal are recorded in the shipped spec.

### CHECKPOINT — loop 3, evaluation 3 (forced by the 3-evaluation rule)

* **Loop identity:** §6 architect implementation review (downstream/additive gate
  of the wrapping `/codex-issue` pipeline), plus its support validations.
* **Window / cumulative:** evaluation 3 of 3 in the first window; cumulative 3.
* **Current SHA / dirty state:** recorded at decision time below.
* **Per-tier counts:** eval 1 — 0 critical, 8 standard. eval 2 — 2 critical,
  5 standard. eval 3 — 1 critical, 0 standard.
* **Affected-class breadth:** 6 areas → 4 → 1.
* **Defect classes:** resolved — checkout binding, event validation, floor
  equality, tombstone reporting, renderer opt-out, provider typing, digest
  derivation, mutation distinctness. **Recurring — the local-bootstrap "has it
  landed" predicate, three times.**
* **Structural-fix rule applied.** That recurrence is the same defect class
  found more than twice, so it stopped being instance-patched: the heuristic
  question ("is this containing ref an integration branch or a mirror of mine?")
  is not decidable locally, and every enumeration of it leaked. It is replaced by
  an invariant that is decidable — *has the introducing commit been shared
  outside the current branch at all?* — with the cost (the bootstrap arm expires
  on first push) stated in the spec rather than engineered around.
* **Trend:** unresolved count 8 → 7 → 1; breadth 6 → 4 → 1; no class being
  instance-patched after the structural fix; highest severity steady at P1 for
  two rounds but on a single, now-structurally-closed class.

### CHECKPOINT DECISION — loop 3, evaluation 3: **CONTINUE**

Owed validation ran first and unconditionally, as required: wave gate at
`f548f70` (exit 0, 9689 passed / 18 skipped, 60 goldens deterministic) and a
delta-scoped repo review.

*Attestation note.* The first collection of that delta review returned
`STATUS: failed`, `SCOPE: (unresolved)` — I had dropped the
`jq -er '.scope' … > "$RUN_DIR/scope"` line from the start block, so the run
directory was incomplete and the collector refused to certify a review that had
in fact completed cleanly. That is the gate behaving correctly; a review nobody
can place is not a review. `last-reviewed-sha` was correctly NOT written, so the
anchor stayed at `ff8e310`. The round was re-run in full with the complete block
(`/tmp/cdx-review.Kkf8n6`) and attested: `SCOPE: branch diff against ff8e310
head=f548f708 dirty=false`. The re-run is the same evaluation, not a new one — a
fresh run directory does not mint a loop.

**Rationale for CONTINUE:** the re-run surfaced one **Critical** (P1) finding —
the reachability probe queried `refs/heads refs/remotes` and omitted `refs/tags`,
so a published tag left the bootstrap claimable. Critical residue may never be
deferred or closed over, so the only legal outcomes were CONTINUE or
ESCALATE-OPEN. A concrete, materially different action existed, so CONTINUE.

**Named finite next correction (taken):** stop enumerating namespaces. This is
the *fourth* leak from this one predicate, and the third was already met with a
structural fix — so the answer was not to add `refs/tags` to the list but to
delete the list: `git for-each-ref --contains <sha>` with no patterns returns
every ref, and full refnames are compared so a tag sharing the branch's name
cannot exempt itself. The enumeration that kept drifting out of step with the
sentence "ANY other ref spends it" no longer exists.

*Process slip, recorded rather than hidden:* this decision was written after the
correction was applied rather than immediately before it. The outcome was not
affected — a Critical finding admits only CONTINUE or ESCALATE-OPEN, and the
concrete action existed — but the ordering the policy specifies is decide-then-mutate.

## Loop 3 — evaluations 4 and 5 (support repo reviews, both delta-scoped)

| Eval | Run dir / scope | Findings | Disposition |
|---|---|---|---|
| 4 | `/tmp/cdx-review.Kkf8n6` — `ff8e310..f548f708` | 1 × **P1**: "Include tags in the spent-bootstrap reachability check… `refs` above omits `refs/tags`" | **fixed** — the namespace enumeration was DELETED rather than extended (`for-each-ref --contains` with no patterns), and full refnames are compared so a same-named tag cannot exempt itself |
| 5 | `/tmp/cdx-review.u80oyH` — `f548f70..f073bf89` | 1 × P2: "Resolve HEAD to its full symbolic ref before filtering… `--abbrev-ref` returns `heads/release`… constructs the nonexistent `refs/heads/heads/release`" | **fixed** — `git symbolic-ref -q HEAD`, which also subsumes the separate detached-HEAD probe |

Eval 5 verified by construction before fixing: in a repo with branch `release`
and a tag `release`, `git rev-parse --abbrev-ref HEAD` really does return
`heads/release`, so the code built `refs/heads/heads/release`. Direction of the
bug is a FALSE REFUSAL (fail-closed), not a bypass — hence P2, not P1.

**The recurring class, and what has been done about it.** Four of the last five
findings sit in one predicate: "has the bootstrap been spent?". Each fix removed
a layer of guessing rather than adding a case —
heuristic branch classification → *deleted*; namespace enumeration → *deleted*;
abbreviated-name parsing → *deleted*. What remains is two git primitives with no
enumeration left to drift: `symbolic-ref` for "which branch am I on" and
`for-each-ref --contains` for "who else has this commit". If this predicate
produces a further finding, the materially different action remaining is to
delete the guard itself and document local `--bootstrap` as an explicit operator
assertion — and if that is also unavailable, `ESCALATE-OPEN`.

## Loop 3 — §6 architect review, round 2 (delta-scoped `f4235b9..00146c5`)

Run dir `/tmp/cdx-gate-review.bn6lKZ`. Attestation `ok:true`, gate `review`,
`parsedVerdict: ISSUES FOUND`, prompt + plan both verified inside the attested
prompt. **8 findings, 3 × P1, 0 false positives** — and this round the reviewer
REPRODUCED its claims rather than reading for them.

| Source ID | Verbatim summary (abridged) | Class | Tier | Disposition |
|---|---|---|---|---|
| ARCH2-01 | "substitution 3 remains replayable… a stale branch can independently recreate both manifests… because the SHAs differ, `--bootstrap` exits 0" | mutation accounting | **Critical** | **fixed** — the landing test now asks about the PATH's history (`rev-list --all --not <own_ref> -- <path>`), not one commit's identity |
| ARCH2-02 | "ambiguous baseline detection covers only `heads`, `tags`, and `remotes`. I reproduced `refs/ambiguous`… `ambiguous~0` also bypasses" | machine-served contract | **Critical** | **fixed** — ask git (`warning: refname … is ambiguous`) instead of enumerating namespaces |
| ARCH2-03 | "HEAD, porcelain, and numstat are not a content fingerprint. Same-line-count rewrites… produce identical before/after values" | mutation accounting | **Critical** | **fixed** — full `git diff` patches + SHA-256 of each untracked file |
| ARCH2-04 | "`_provider_call` catches only `Exception`; a provider raising `SystemExit(0)` terminates the wave with status 0. I reproduced this" | capability reachability | Standard | **fixed** — catch `BaseException`; attribute access moved inside the guard |
| ARCH2-05 | "`unittest.SkipTest` is re-raised rather than converted… ordinary CI can leave a golden unrendered" | capability reachability | Standard | **fixed** |
| ARCH2-06 | "PR checkout binding tests parent membership, not exact parentage" (octopus merge accepted) | machine-served contract | Standard | **fixed** — exactly two parents, equal to {head, target} |
| ARCH2-07 | "substitution 8's `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1` refutation is incorrect… all 9,712 nodes still collected and the two cited modules passed 50/50" | CI configuration | Standard | **fixed — my refutation was wrong** |
| ARCH2-08 | "the bare-`wave` negative asserts only exit status 2, not `BASELINE_EVENT_INVALID`" | test coverage | Standard | **fixed** |

**ARCH2-07 — a refutation of mine that did not survive measurement.** Last round
I refuted the flag by reasoning that `anyio` registers a `pytest11` entry point
and two modules drive async code through `anyio.run`, so disabling autoload would
change the validated configuration. That was an INFERENCE presented as a
measurement. Re-measured here: with `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`, all 9,712
nodes still collect and both modules pass 50/50 — `anyio.run()` is a plain
function, unrelated to the plugin's marker and fixtures, which this repo never
uses. The flag is now set and the spec records the correction rather than the
original claim. (The three `BOOMI_*` omissions and the two alias diagnostic codes
were accepted by the reviewer and stand.)

**Recurring classes, both structurally closed this round:**
* *Enumeration drifting out of step with its own sentence* — third occurrence
  (namespaces in the ambiguity probe, after namespaces in the landing probe).
  Fixed by deleting the enumeration and reading git's own answer.
* *Identity-based reasoning about a shared artifact* — the landing test matched a
  COMMIT where the contract is about a FILE. Fixed by asking about the path's
  history.

### CHECKPOINT — loop 3, second window: **CONTINUE**

Owed validation ran first (wave gate at the tip, exit 0). Three Critical findings
means DEFER and CLOSE are both illegal; the choice was CONTINUE or
`ESCALATE-OPEN`. Concrete, materially different structural actions existed for
every one and were applied, so CONTINUE. Trend: the support-review axis converged
to clean (8 → 7 → 1 → 1 → 0); the §6 axis held at 8 but shifted from
read-the-code findings to reproduced ones, which is a different and deeper class
of evidence rather than the same defects recurring. Named finite next step: one
more §6 delta round to test whether the two structural fixes held.

## Loop 3 — support repo reviews, evaluations 8 and 9

| Eval | Scope | Findings | Disposition |
|---|---|---|---|
| 8 | `/tmp/cdx-review.Jrr8nU` — `00146c5..cd58428` | **P1** untracked DIRECTORIES collapse to one `?? dir/` entry; P2 ambiguity probe depends on `core.warnAmbiguousRefs` and English stderr; P2 `--all --not <own_ref>` subtracts merged-in commits | all **fixed** (`--untracked-files=all -z`; forced `core.warnAmbiguousRefs=true` + `LC_ALL=C`; positive per-ref roots) |
| 9 | `/tmp/cdx-review.4nzbJG` — `cd58428..46ad117` | **P1** default history simplification prunes a `merge -s ours` addition; P2 `-z` output captured with `text=True`; P2 per-ref diagnostic is O(refs) processes | **fixed** — the first and third by REMOVING the mechanism (below); the second by capturing bytes |

### The recurring class, and the decision that ends it

The local-bootstrap "has this ledger landed?" predicate produced a finding in
**eight consecutive formulations**:

1. ancestry-only → re-claimable forever after landing
2. commit-count → rejected ordinary multi-commit development of the introduction
3. `*/<branch>` mirror exemption → reopened the hole on `dev`, where this repo lands
4. ref-namespace enumeration → missed `refs/tags`
5. `rev-parse --abbrev-ref` → returns `heads/<name>` when a tag shares the name
6. matching the introducing COMMIT → a branch that recreates the ledger has different SHAs
7. `--all --not <own_ref>` → subtracts merged-in commits, hiding the other branch's addition
8. default path-limited `rev-list` → prunes an addition that arrived via `merge -s ours`

Each was verified and each fix was structural rather than a patch, and it kept
leaking anyway. The reason is not implementation quality: **locally the operator
chooses the baseline**, so no rule can separate "legitimately introducing the
ledger" from "asserting a stale baseline", and erring toward refusal blocks the
introduction the exception exists for. The question is ill-posed in that context.

`CLAUDE.md`: *"recurring again after the structural fix with no materially
different action left → ESCALATE-OPEN."* A materially different action DID remain,
flagged two checkpoints earlier: **delete the local enforcement.** Applied —
`_refuse_stale_local_bootstrap` is gone. `--bootstrap` is still required locally
(the operator must mean it), the run now prints `WARNING — a local --bootstrap is
an OPERATOR ASSERTION, not a verified one`, and the spec records all eight failed
formulations plus the impossibility argument so a ninth is not attempted.

Nothing that any acceptance criterion depends on was weakened: the enforcement
that matters is in the `ci` arms, where the baseline comes from the platform and
not from the person being checked — `push` compares against the branch tip it
builds on, `pull_request` additionally requires the target to carry no manifests.
Those have been clean for several rounds. A local `wave --bootstrap` still runs
the suite, all 60 goldens and the determinism check; only the transition portion
is unvalidated, and in a genuine bootstrap there is no transition to validate.

Ten pytest node ids were retired with it — tombstoned explicitly in the same
change, which the gate reported row by row and accepted as a legal transition
(9,725 preserved, 2 appended, 10 tombstoned; `minimum_active` 9,715).

## Loop 3 — §6 architect review, round 3 (delta-scoped `00146c5..2364447`)

Run dir `/tmp/cdx-gate-review.Im48V4`. Attestation `ok:true`, gate `review`,
`parsedVerdict: ISSUES FOUND`. **The removal was ACCEPTED:** *"The local-bootstrap
removal is a legitimate structural resolution. A local operator already controls
`--base`; local ref topology cannot authoritatively prove 'landed.' Keeping that
authority in push/PR CI, with an explicit local assertion and warning, is the
sound boundary."* 5 findings, 2 Critical, all independently reproduced by the
reviewer, 0 false positives.

| Source ID | Verbatim summary (abridged) | Class | Tier | Disposition |
|---|---|---|---|---|
| ARCH3-01 | stores complete staged/unstaged patches then prints them verbatim on `WORKTREE_DIRTY`; *"I reproduced an uncommitted `TOKEN=SUPER_SECRET_VALUE` appearing in the diagnostic"* | **secrets/security** | **Critical** (anchor: secrets) | **fixed** — patches are SHA-256'd; the fingerprint discloses nothing |
| ARCH3-02 | fingerprint still fails open: a stable errno token hides an unreadable file's content; default `git diff` abbreviates binary blob ids so a 7-char prefix collision is invisible | mutation accounting | **Critical** | **fixed** — `--binary --full-index` digests, and an unhashable file now FAILS CLOSED |
| ARCH3-03 | *"I constructed a commit with exactly `{head,target}` as parents but the target's tree… it was accepted"* | machine-served contract | Standard | **fixed** — bind to the event's `merge_commit_sha` when GitHub supplies it |
| ARCH3-04 | provider-originated `GateFailure(..., 0)` re-raised and exits green | capability reachability | Standard | **fixed** — every provider exception becomes status-1 |
| ARCH3-05 | *"the exact requested range is not a legal manifest transition"* — a row appended then tombstoned inside a 4-commit push reads as born-tombstoned | machine-served contract | Standard | **fixed** — an appended row may be born tombstoned; the tree checks already prove the artifact is absent |

**ARCH3-05 is the one with the widest blast radius** and it was verified directly:
`manifests --base 00146c5` really did exit 2 on this branch. The born-tombstoned
prohibition made ordinary multi-commit pushes illegal while every individual
commit transition was legal — a rule that would have fired on real pushes, not a
corner case. Relaxing it costs nothing because a tombstoned row is separately
required to have no golden file and no collecting node id.

Verified before fixing: ARCH3-01 reproduced on this repo (`SECRET PRESENT IN
FINGERPRINT: True`); ARCH3-05 reproduced by running the gate over the stated range.

Also corrected: three documentation lines the reviewer flagged as retaining
pre-correction claims (the local arm "closed by reachability"; the
immediate-introduction rationale; the CI env list still saying four omissions).

## Loop 3 — support repo review of the ARCH3 corrections

Run dir `/tmp/cdx-review.A3szGV`, scope `2364447..7635193`. **3 findings, none
Critical** — the first round in five with no P1.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
|---|---|---|---|
| SUP3-01 | "Refuse PR merges without an authoritative merge SHA… falls through to the parent-only check… the exact case this change intends to close" | Standard | **fixed** — no merge sha ⇒ refuse; the parentage fallback is deleted outright |
| SUP3-02 | "Make the unreadable-file test privilege-independent… when the suite runs as UID 0… `pytest.raises` fails, making the required suite red despite correct behavior" | Standard | **fixed** — the read failure is simulated, not produced with `chmod(000)` |
| SUP3-03 | "Align the transition guide with born-tombstone behavior" | Non-blocking | **fixed** |

SUP3-02 is worth noting: it is a finding about the SUITE being red on a runner
for a reason unrelated to the behaviour under test — container CI commonly runs
as root, where mode bits do not deny access. It would have turned the required
check red on its first real run, which is precisely the failure class this slice
exists to prevent, arriving through its own test.

## Loop 3 — support repo review of the SUP3 corrections

Run dir `/tmp/cdx-review.OWssCD`, scope `7635193..a44ee53`. **1 finding**, P1 —
and it corrects the fix from the round before it.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
|---|---|---|---|
| SUP4-01 | "Bind PR merge checkouts to GITHUB_SHA… GitHub emits a valid `pull_request` payload with nullable `merge_commit_sha` set to null… this branch rejects the valid checkout before any tests run" | **Critical** (P1; false-refusal direction — would have made every affected PR red) | **fixed** |

The two rounds pulled in opposite directions and both were right: SUP3-01 said
*don't fall back to parentage*, SUP4-01 says *don't require the nullable payload
field*. The resolution is a third source that is neither — `GITHUB_SHA`, which
Actions always sets and which for a `pull_request` IS the merge commit it checked
out. `merge_commit_sha` is kept as a secondary; parentage is gone.

The workflow now passes `GITHUB_SHA: ${{ github.sha }}` explicitly rather than
relying on the ambient value, and the affected tests `monkeypatch.delenv` it —
because this suite RUNS inside Actions, where it is always populated, and a test
that reads the ambient environment passes locally and fails on the runner. Both
directions verified: 175 passed with `GITHUB_SHA` set to a foreign sha, 241
passed with it unset.

## Loop 3 — support repo reviews of the GITHUB_SHA corrections

| Eval | Scope | Findings | Disposition |
|---|---|---|---|
| — | `/tmp/cdx-review.k1v3oG` — `a44ee53..ef88f80` | P2: "Prefer GITHUB_SHA over the payload merge SHA… these lines keep `context['merge_sha']` and never consult the explicitly supplied `GITHUB_SHA`" | **fixed** — the comment said "GITHUB_SHA FIRST" while the code checked the payload first; env is now genuinely primary, with the precedence pinned by a test |
| — | `/tmp/cdx-review.vKCjVk` — `ef88f80..7984de5` | **P1**: "Keep the outer GITHUB_SHA out of synthetic PR checks… This makes the required CI suite fail" | **fixed** |

**The P1 is the most consequential finding of the whole slice after the Python
3.11 discovery.** Making `GITHUB_SHA` primary meant that two tests driving
`check_checkout_matches_event` against SYNTHETIC repositories would read the
OUTER checkout's sha — and this suite runs inside the very workflow that exports
it. Reproduced exactly: under `GITHUB_SHA=1111…`, `2 failed, 173 passed`. The
required check would have gone **red on its first real run**, for a reason
unrelated to the behaviour under test.

Fixed with an **autouse fixture** that scrubs `GITHUB_SHA` for the whole module,
not a `delenv` in each affected test — per-test scrubbing is the same enumeration
I had already got wrong twice in this file. Tests that want the variable set it
explicitly, which still wins because `monkeypatch.setenv` runs after the fixture.
Verified in both directions: 175 passed with `GITHUB_SHA` populated, 241 passed
with it unset, and the final wave gate was run with it populated so the recorded
evidence matches how CI will actually invoke it.

## Loop 3 — §6 architect review, round 4 (delta-scoped `2364447..1627a96`)

Run dir `/tmp/cdx-gate-review.2FPMlo`. Attestation `ok:true`, gate `review`,
`parsedVerdict: ISSUES FOUND`. The reviewer independently re-ran the focused
suite (175 passed with ambient `GITHUB_SHA`) and both manifest ranges before
reporting. 3 findings, 1 Critical.

| Source ID | Verbatim summary (abridged) | Class | Tier | Disposition |
|---|---|---|---|---|
| ARCH4-01 | "unreadable directories still bypass mutation accounting… `git status` and `git diff` return 0, warn on stderr, and omit the files. A chmod/write/chmod mutation produced an identical fingerprint" | mutation accounting | **Critical** | **fixed** — ANY stderr from a fingerprinting command is now a refusal |
| ARCH4-02 | "provider-controlled behavior can still exit green… an exception whose `__str__` raises `SystemExit(0)`… exited the process with status 0" | capability reachability | Standard | **fixed** — one encompassing boundary around the whole fingerprint phase |
| ARCH4-03 | "born-tombstoned rows bypass the retirement audit… `manifests --base 00146c5` passed without reporting `pytest-009719`, `009720`, `009723`" | mutation accounting | Standard | **fixed** — they stay out of the floor arithmetic (they were never active) but are now reported |

ARCH4-01 verified against real git before fixing: with an unreadable directory,
`status_rc=0` and stderr `could not open directory 'locked/': Permission denied`.
The fix refuses on any stderr rather than matching known warning strings —
enumerating "which git messages matter" is precisely the defect shape this file
has produced repeatedly, and on a clean checkout these commands emit nothing, so
anything at all means the snapshot is incomplete.

ARCH4-03 verified after fixing: the three row ids the reviewer named now appear
in the audit trail on the real range, tagged `(appended already retired)`.

On ARCH4-02 I note the boundary is not a claim to defend against a hostile
provider — the #153 provider is repo-owned code, and this repo has already
recorded that a hostile registered component is inside the trust boundary
(#145 threat model). It is fixed because it is cheap and because a gate exiting
green by accident is the single outcome it must never produce, not because the
threat model changed.

## Loop 3 — support repo review of the ARCH4 corrections

Run dir `/tmp/cdx-review.YSPl3A`, scope `1627a96..8d70b8e`. 2 findings, 1 P1 —
both consequences of the previous fix.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
|---|---|---|---|
| SUP5-01 | "the new stderr policy can make the mandatory gate unusable on benign Git diagnostics… `warning: confstr() failed … using /tmp instead`… `_status()` rejects every `manifests`, `ci`, or `wave` invocation in that environment" | **Critical** (false-refusal; disables the required check) | **fixed** — narrow match on access-failure signals |
| SUP5-02 | "Make the permission test independent of root privileges… `chmod(000)` does not make this directory unreadable" as UID 0 | Standard | **fixed** — the warning is simulated |

**SUP5-01 reverses my own previous decision, and the reversal is the correct
one.** One round earlier I refused on ANY stderr and argued explicitly against
enumerating warnings, on the grounds that "a spurious refusal is loud and
fixable; a silent gap is neither". That reasoning ignored the asymmetry that
actually governs here: over-matching makes the REQUIRED check refuse every
invocation in an affected environment — the gate cannot run at all — whereas
under-matching leaves only the narrow residual that already existed. A gate that
cannot run protects nothing. The enumeration is now deliberate and documented as
such, including why the earlier reasoning was wrong.

My environment does not emit that warning (probed: both commands produce empty
stderr), so no run recorded here was affected — but the policy was fragile
elsewhere, which is exactly what a reviewer is for.

SUP5-02 is the THIRD finding in this slice of the form "this test passes locally
and fails on a runner" (after the gcloud-gated skip and the ambient
`GITHUB_SHA`). All three are now simulated rather than environment-dependent.

## Loop 3 — §6 architect review, round 5 (delta-scoped `1627a96..0602f20`)

Run dir `/tmp/cdx-gate-review.HLZL2u`. Attestation `ok:true`, `parsedVerdict:
ISSUES FOUND`. 2 findings, both P1, both REPRODUCED by the reviewer with git
source citations.

| Source ID | Verbatim summary (abridged) | Class | Tier | Disposition |
|---|---|---|---|---|
| ARCH5-01 | "The stderr enumeration still fails open… a tracked-path symlink loop produced status 0 and `dir/file: Too many levels of symbolic links`… its directory iterator can treat `readdir()` errors as EOF without any diagnostic" | mutation accounting | **Critical** | **claim corrected, mechanism kept** |
| ARCH5-02 | "The fingerprint boundary still trusts provider-controlled `GateFailure`… a tuple subclass whose `__len__` raises `GateFailure(..., status=0)` reaches `main()`, which returns 0; I reproduced the full orchestrated path with `main_status=0`" | capability reachability | **Critical** | **fixed structurally** |

**ARCH5-02 — fixed with an invariant, not another guard.** Guarding individual
raise sites had already leaked twice. The status is now coerced in
`GateFailure.__init__` (anything not in {1,2} becomes 1), so no present or future
path can exit green through a failure object. Additionally: the outer boundary
raises a FRESH failure and never formats the exception object (a `__repr__` could
raise `SystemExit(0)` out of the handler), and provider outputs are validated as
EXACT built-ins (`type(x) is tuple/str/bytes`) so a subclass's `__len__` or
`__eq__` never runs.

**ARCH5-01 — the over-claim was the defect, not the mechanism.** The reviewer's
second citation settles it: git's directory iterator can omit a subtree with NO
diagnostic at all, so *no* stderr-based policy can be complete. The proposed
alternative — an independent byte-path `os.scandir` inventory reconciled against
the index — is a new subsystem, would need its own review rounds, and is not
required by any acceptance criterion.

What made this a finding was that the docs claimed completeness. The gate's
read-only property is STRUCTURAL — it writes only under `tempfile.mkdtemp()`,
runs children with `PYTHONDONTWRITEBYTECODE=1` and `-p no:cacheprovider`, and
never invokes a mutating git command. The fingerprint is a runtime cross-check of
that structure. It now says so in the code, in the gate spec, and in the README:
*"treat a pass as 'nothing this check can see changed'"*, with the two git
behaviours named. Six review rounds went into this mechanism; the residue
disappears once it stops promising more than it can deliver, and nothing else in
the slice depends on the stronger claim.

## Loop 3 — support repo review of the ARCH5 corrections

Run dir `/tmp/cdx-review.VoZawR`, scope `0602f20..4ee5365`. 3 findings, 2 P1.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
|---|---|---|---|
| SUP6-01 | "Avoid provider-controlled class metadata in the final handler… evaluating `type(exc).__name__` invokes provider code inside this last-resort handler" | P1 | **fixed** — the handler now uses a FIXED message and inspects nothing |
| SUP6-02 | "Validate the status type before preserving it… an `int` subclass whose underlying value is 0 but whose `__eq__` reports equality with 1" | P1 | **fixed** — `type(status) is int` and a LITERAL is stored |
| SUP6-03 | "Preserve specific fingerprint failures at the phase boundary… emits `PLAN_FINGERPRINT_MISMATCH … GateFailure` instead of the documented `PLAN_FINGERPRINT_PENDING`" | P2 | **fixed** — a real `GateFailure` is re-raised unchanged, which is safe precisely because the constructor now guarantees its status |

SUP6-03 was a REGRESSION I introduced one round earlier while hardening the same
boundary, and my own test missed it because it called
`run_plan_fingerprint_checks` directly rather than through the boundary. The
boundary is now extracted as `run_fingerprint_phase` so it is unit-testable, and
the pending/mismatch diagnostics are pinned.

### Boundary note for any further adversarial-provider findings

SUP6-01/02 and ARCH5-02 are all of one shape: a HOSTILE provider subverting the
gate's exit path via `__name__`, `__eq__`, `__len__`, `__repr__`. They were fixed
because each fix is a cheap invariant that improves the code regardless — not
because the threat model changed.

This repository has already ruled on that class, in #145 §7, on the
recommendation of a Codex session and a separate analysis: *"an adversary who can
write a metaclass `__hash__` can write `_STASH = {}`"*, and — Codex's own words —
*"protecting only against malicious recipe classes while trusting other Python in
the same process is not a coherent security boundary."* The #153 fingerprint
provider is registered, reviewed Python running in the gate's own process; anyone
who can give it a hostile metaclass can equally put `sys.exit(0)` at its module
scope, or simply have it return correct-looking values for a wrong plan. Further
findings of this shape are dominated by a channel the design has already accepted
and will be recorded as `severity-refuted` against that precedent rather than
patched indefinitely.

## Loop 3 — support repo review of the SUP6 corrections

Run dir `/tmp/cdx-review.41XO9G`, scope `4ee5365..4880611`. 1 finding, P1.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
|---|---|---|---|
| SUP7-01 | "Re-sanitize GateFailure objects at the provider boundary… `GateFailure` remains mutable and subclassable, so provider code can set `status = 0`… re-raising it unchanged then lets `main()` return 0" | P1 | **fixed — by a different mechanism than proposed** |

**The proposed remedy conflicts with the previous round's.** SUP6-03 required
that real diagnostics pass through the boundary unchanged (`PLAN_FINGERPRINT_PENDING`
must stay itself); SUP7-01 asks for the boundary to reconstruct sanitized
failures, which would discard exactly those diagnostics. Both asks cannot be
satisfied at the same place.

They are satisfied at DIFFERENT places. The diagnostic must survive the
boundary; the STATUS must be trustworthy at the exit. So the invariant moved to
where the value is consumed: `exit_status_for()` recomputes it in `main()` —
`type(status) is int and status == 2`, else 1 — so nothing that happens to the
exception in between can produce a green exit, whether by post-construction
mutation, a subclass, or an int subclass with a lying `__eq__`. A constructor
invariant only holds at construction; an exit-point invariant holds always. The
message fields are `str()`-coerced at the same point.

This is also the better fix for ordinary bugs, not only adversarial ones, which
is why it was taken rather than refuted — even though the finding's *premise* (a
hostile registered provider) falls inside the boundary this repo already ruled
on in #145 §7, recorded above.

## Loop 3 — support repo review of the SUP7 correction

Run dir `/tmp/cdx-review.7Gl834`, scope `4880611..b2aeea3`. 1 finding, P1 —
against the `str()` coercion I added in response to the round before it.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
|---|---|---|---|
| SUP8-01 | "Do not call untrusted `__str__` on the failure path… a `str` subclass… whose `__str__` raises `SystemExit(0)`, this new eager conversion escapes before `exit_status_for` runs" | P1 | **fixed structurally** |

**Four consecutive rounds, four different special methods.** `__name__` via a
metaclass; `__repr__` in a formatter; `__eq__` on an int subclass; now
`__hash__`/`__str__` on a str subclass. Each fix was correct and each created the
surface for the next, because they all shared one premise: that the exit path
could safely *touch* provider-controlled values while rendering a message. There
is always another special method, so patching them individually cannot terminate.

The structural answer is to remove the dependency rather than harden each hop:
**decide first, render second.** `main()` now computes the exit status before any
message is built — from a guarded attribute read, a `type()` check and a
comparison against a literal, none of which can execute foreign code — and then
renders inside its own guard. Rendering may fail freely; the worst outcome is a
less informative line, never a green exit. Pinned by
`test_a_failure_whose_diagnostic_explodes_still_exits_nonzero`, which drives the
real `main()` with a failure whose `__str__` and `__format__` both raise
`SystemExit(0)` and asserts it returns 1.

Should this class recur again, the disposition is `severity-refuted` against the
#145 §7 precedent recorded above, not a fifth dunder patch.

## Evidence

- Wave gate at tip: `wave --base 9080e3c --bootstrap` → **exit 0** (run 1, 9662 nodes;
  run 2 after fixes, 9664 nodes). Suite 9644 passed / 18 skipped / 0 failed in 12:42 on 3.11.
- 60/60 goldens render byte-exactly and deterministically across two isolated
  children with different `PYTHONHASHSEED`.
- Darkness: `git diff <base> -- src` empty; `-- tests/fixtures/golden_xml` empty;
  `-- examples` empty; root modules empty.
- #149 freeze: `inv.compare(build_inventory(), load_baseline()).empty() is True`.
- Gate tests: 185 passed.

---

## §6 architect review — round 6 (RUN_DIR `/tmp/cdx-gate-review.avvId7`, attested `ok:true`)

Verdict `ISSUES FOUND`, one finding. The round also **closed the exit-status class**:
"all seven added adversarial regressions pass, and I found no nonzero-to-zero route
within the recorded provider trust boundary" — SUP7-01/SUP8-01 are settled, and the
fingerprint's best-effort scoping was accepted as reasonable.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| ARCH6-01 | "The replacement 'structural read-only' invariant is not enforced — `scripts/wave_gate.py:428`. `tempfile.mkdtemp()` at line 1947 honors `TMPDIR`; with `TMPDIR=<repo>`, scratch is created inside the repository and deleted before the final fingerprint. Reproduced: `inside_repo=True`, while `fingerprint_equal_after_cleanup=True`. This violates the plan's explicit outside-repository requirement." | P2 → **standard** (blocking class: *runtime behavior*) | **fixed** |

**Anchor for the tier.** Source label P2, and no critical anchor applies: no secret is
exposed, no data lost, no mutation miscounted. It is nonetheless blocking — the gate's
read-only property is runtime behavior, and I had *substituted this very invariant* for
the fingerprint-completeness claim I withdrew in round 5. A withdrawn claim replaced by
a false one is worse than the original gap.

**Verification, independently reproduced before fixing** (`TMPDIR=$(pwd)`):
`scratch: <repo>/wave-gate-s31e0itu`, `inside_repo: True`. Confirmed.

**Fix — `make_scratch_dir(repo)` (`scripts/wave_gate.py`).** `mkdtemp()`, then compare
the *realpath* of the result against the realpath of the repo root; if it is the root or
underneath it, remove the directory and raise `SCRATCH_INSIDE_REPO` (status 2). The
invariant is now enforced at the only place scratch is created, rather than asserted in
a comment above it. Docs updated: the error code is listed, and the
`ENDGAME_VERIFICATION_GATE.md` "read-only is STRUCTURAL" paragraph now says *and
ENFORCED*, naming the `TMPDIR` route it closes.

**Regression:** `test_scratch_inside_the_repository_is_refused` — asserts the refusal,
`status == 2`, that no `wave-gate-*` directory is left inside the tree, and that an
outside-the-repo `TMPDIR` is still accepted and really resolves outside.

*Test-harness note, worth recording because it nearly produced a false green:*
`tempfile.gettempdir()` **memoizes**, so setting `TMPDIR` inside a warm pytest process
is inert — the first version of this test passed the env var and did not raise. That is
a property of the *test process*, not of the gate: a real gate run is a cold process
that resolves `TMPDIR` on first use, which is exactly the reviewer's reproduction. The
test now clears `tempfile.tempdir` so it exercises the production derivation. Had I read
the initial non-raise as "not actually reachable," I would have refuted a true finding
with an artifact of my own harness — the failure mode recorded in
[[verify-claims-and-harnesses-by-measuring]].

---

## Stage-2 repo review — round 9 (RUN_DIR `/tmp/cdx-review.j6wCGu`, `STATUS: completed`)

`SCOPE: branch diff against 8ef5a6d3e4142e27d46bc38616b5114de5d47e54 (8ef5a6d)
head=297734943b131ce775387ae9076d606bb675b690 dirty=false` — the fix-only delta.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX9-01 | "[P1] Use filesystem identity for scratch containment — `scripts/wave_gate.py:2005`. On a case-insensitive filesystem such as default macOS, `realpath()` preserves path spelling, so a repository reported as `/Users/.../repo` and `TMPDIR=/users/.../repo` refer to the same directory but fail this case-sensitive comparison. The gate then runs and cleans its scratch inside the worktree, leaving the closing fingerprint unchanged—the exact fail-open case this change intends to reject." | **P1 → critical** (source label P1; blocking class *runtime behavior*) | **fixed structurally** |

**Independently reproduced before any edit**, on this machine:

```
repo                       : /Users/gleb/.../boomi-mcp-server
scratch                    : /users/gleb/.../boomi-mcp-server/wave-gate-j9put4ce
lexical check would refuse : False
samefile(parent, repo)     : True
PHYSICALLY inside the repo : True
```

The check I had just added said "outside" about a directory physically inside the
worktree. Fail-open, exactly as described.

**This is the structural-fix rule, not a third patch.** ARCH6-01 and CDX9-01 are one
defect class: *deciding a filesystem question by comparing strings*. Round 6 replaced a
comment with a lexical test — still a spelling comparison, so still an enumeration of
the spellings I happened to think of. The invariant is now derived from the runtime
authority: `_refuse_scratch_inside_repo` walks the scratch path's ancestors comparing
`(st_dev, st_ino)` against the repo root's. Inode identity is the filesystem's own
answer and is immune to case-folding, symlinks, and every other spelling variance —
there is no next spelling to miss. An ancestor that cannot be stat'd is not evidence of
safety, so it fails closed as the new `SCRATCH_CONTAINMENT_UNPROVEN` (status 2).

**Regressions:**
- `test_scratch_containment_is_decided_by_inode_not_by_spelling` — constructs the
  same-inode/different-spelling `TMPDIR` and asserts `SCRATCH_INSIDE_REPO`. It gates on
  a runtime *probe* of case-insensitivity (create `CaseProbe`, ask whether `caseprobe`
  is `samefile`), never on `sys.platform`: the hazard is a property of the filesystem,
  and a case-sensitive one cannot express it. Runs on macOS; skips on the Linux runner
  with that reason recorded (skip budget 18 → 19 of 30).
- `test_scratch_containment_that_cannot_be_proven_fails_closed` — an unstattable repo
  root yields `SCRATCH_CONTAINMENT_UNPROVEN`, not a pass.

**`test_every_diagnostic_code_the_gate_can_raise_is_documented` failed on the new code**
until `ENDGAME_VERIFICATION_GATE.md` listed it — the doc-coverage invariant working as
designed, caught before the gate ran.

## Stage-2 repo review — round 10 (RUN_DIR `/tmp/cdx-review.4moHcY`, `STATUS: completed`)

`SCOPE: branch diff against 2977349 head=4784406d3538de98be23b782dfc6039f202123f4 dirty=false`

> "The inode-based ancestor check correctly handles alternate path spellings while
> preserving fail-closed behavior. The accompanying tests, manifest floors, and
> diagnostic documentation align with the implementation."

**No findings.** Zero raw findings, so no ledger rows.

## CHECKPOINT — Stage-2 repo review loop, evaluation 9

- **Loop identity:** Stage-2 repo Codex review (inner loop), slice #152.
- **Window / cumulative:** evaluation 3 of window 3 / cumulative 9.
- **SHA at decision:** `4784406d3538de98be23b782dfc6039f202123f4`, `dirty=false`.
- **Validation owed and completed BEFORE this decision** (per the ordering rule — a
  correction's validation is never budget-gated): darkness proof, wave gate exit 0
  (9717 passed / 18 skipped, 60 goldens deterministic), and the round-10 fix-only
  review, which came back clean.
- **Per-tier counts:** critical 0 unresolved (CDX9-01 fixed and validated); standard 0
  unresolved; non-blocking 0 outstanding.
- **Breadth:** one class touched (scratch containment), now closed at its authority.
- **Defect classes — new / resolved / recurring:** new 0; resolved 1 (*spelling
  comparison standing in for a filesystem fact*, which covered both ARCH6-01 and
  CDX9-01); recurring 0. The recurrence of that class at round 9 is what triggered the
  structural fix rather than a third instance patch, and round 10 confirms it closed.
- **Trend:** highest unrefuted severity P1 → none; unresolved count 1 → 0; breadth 1 → 0.
  All three improved, none worsened.
- **OUTCOME: `CLOSE-CLEAN`.** No blocking residue; the current tree is covered by the
  QA/darkness proof, the full wave gate, and a clean delta-scoped review. Nothing is
  deferred, so no follow-up issue is owed.

Stage 2 is satisfied at `4784406`. The §6 architect gate is a separate loop with its own
fresh window and is still owed.

---

## §6 architect review — round 7 (RUN_DIR `/tmp/cdx-gate-review.kaQizX`, attested `ok:true`, `turnToken:2`, `parsedVerdict: ISSUES FOUND`)

*Attestation note: the first turn on the primary prompt ended `{"status":"failed"}` with
only a reasoning preamble; the single sanctioned in-session retry produced the review.
The collector certified `turnToken:2` — this is the documented case where the retry is
the turn that certifies, and the record still binds prompt + plan bytes.*

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| ARCH7-01 | "[P1] The containment decision is vulnerable to path retargeting — `scripts/wave_gate.py:2008`. `_refuse_scratch_inside_repo()` validates the resolved directory once, but `make_scratch_dir()` returns the original, potentially symlink-bearing path. During pytest collection, imported test code can retarget a user-owned `TMPDIR` symlink from an outside directory to the repository and create the same scratch basename… I reproduced `later_write_inside_repo=True` followed by `repo_equal_after_cleanup=True`… Later I/O and cleanup must be anchored to the verified directory object—such as descriptor-relative, no-follow operations—not the mutable pathname." | source P1 → **split**: the path-mismatch half **fixed**; the descriptor-relative demand **severity-refuted** | **fixed (partial) + severity-refuted (remainder)** |

The finding bundles two claims. They get different answers.

**Accepted and fixed — "verified one path, returned another."** This is a real defect
independent of any attacker: `make_scratch_dir()` proved `realpath(candidate)` was
outside the tree and then handed back `candidate`. With `TMPDIR` a symlink those are
different objects, and only one of them was ever checked. `make_scratch_dir()` now
resolves once and returns the RESOLVED path, so the directory written to is the object
the check cleared. Pinned by
`test_the_scratch_path_returned_is_the_path_that_was_verified`, which builds the
finding's own scenario — symlinked `TMPDIR`, repointed at the repo after the check, with
the basename recreated inside — and asserts the write still lands outside.

**Refuted — the demand for descriptor-relative, no-follow I/O.** The attacker this
requires is *arbitrary code execution during pytest collection*. That attacker is inside
the trust boundary by construction: the gate's entire purpose is to EXECUTE this
repository's test suite. And the symlink is not load-bearing for the stated impact.
Measured directly against `_status`/`check_worktree_unchanged`, with no symlink, no
`TMPDIR` and no scratch directory in play:

```
wrote_inside_repo_during_run   : True
fingerprint_equal_after_cleanup: True
check_worktree_unchanged       : PASSED (blind)
modify_then_restore blind      : True
```

A before/after snapshot is structurally blind to any write undone before the closing
snapshot. So `repo_equal_after_cleanup=True` is reachable by simply writing a file and
deleting it — the retargeting adds no capability. Hardening the symlink route would
close one path out of unboundedly many while leaving the capability untouched: an
enumeration standing in for an invariant, which is the class this project has a standing
rule against, and the same class ARCH6-01/CDX9-01 were just resolved under.

The plan's requirement at line 97 is that the gate "write temporary output outside the
repository" — a constraint on where the GATE writes. That requirement is now enforced
and the used path is the proven one. It does not extend to defending the tree from the
code the gate is designed to run.

**Recorded rather than argued once:** `ENDGAME_VERIFICATION_GATE.md` now states the
boundary outright, with the measurement above — the fingerprint is hygiene against the
gate's own accidental writes and a misconfigured `TMPDIR`; test code is trusted because
it is executed; the gate defends the tree against ITSELF. A claim that is written down
does not need re-litigating each round.

The reviewer separately confirmed `SCRATCH_CONTAINMENT_UNPROVEN` "correctly fails closed
as a contract error with status 2" and "no other plan contradiction in this delta."

---

## §6 architect review — round 8 (RUN_DIR `/tmp/cdx-gate-review.TikQjD`, attested `ok:true`, `turnToken:2`, `parsedVerdict: ISSUES FOUND`)

*Round 8 was the round where I explicitly asked the reviewer to attack my own refutation.
It did, and it was right.*

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| ARCH8-01 | "[P1] The returned `realpath` remains a mutable pathname, not the verified directory object — `scripts/wave_gate.py:2015`. A separate sibling process can rename the resolved scratch parent, replace it with a symlink to the repository, and create the same scratch basename there… A safe temporary-repository reproduction produced `later_write_inside_repo=True` and `repo_equal_after_cleanup=True`. **This requires no execution inside the gate's process tree, so the refutation is incorrect.**… descriptor-relative, no-follow I/O and cleanup are still required." | P1 → **critical** (source label P1; blocking class *runtime behavior*) | **fixed structurally** |
| ARCH8-02 | "Consequently, the structural guarantee documented at `ENDGAME_VERIFICATION_GATE.md:208-231` overstates what the code enforces." | **standard** (blocking class: *machine-served schemas/contracts* — this file is the gate's published contract) | **fixed** |

### My round-7 refutation was wrong, and here is the specific error

I refuted the descriptor-relative demand on the ground that the attacker needed
*arbitrary code execution during pytest collection*, i.e. inside the trust boundary. That
premise was false. The capability actually required is **any process running as the same
user, concurrently** — it renames the verified parent and drops a symlink in its place,
entirely outside the gate's process tree. My measurement (write-then-delete defeats the
fingerprint) was correct and remains true, but it established a different proposition
than the one I used it for: it showed the *impact* was reachable another way, not that
*this* route needed in-tree execution. I generalised from the wrong half of my own
evidence.

Recorded because the shape recurs: **a refutation is a claim and needs the same
verification as a finding.** I verified the measurement and did not verify the premise it
was attached to.

### The fix — an invariant at one chokepoint, not hardened call sites

`make_scratch_dir` now returns a `_ScratchDir`: the resolved path plus an open descriptor
on the directory that passed containment. `__fspath__` re-proves `(st_dev, st_ino)`
identity every time the value becomes a string, so every existing
`os.path.join(...)`/`shutil.rmtree(...)` call revalidates without being rewritten — the
alternative was hardening three call sites, i.e. an enumeration that the next write site
escapes. The gate's own writes go through `open_for_write()` (`dir_fd=`), which cannot be
redirected at all. `release()` returns `None` when the binding broke, so cleanup removes
NOTHING through a changed name — a blind `rmtree` there would delete a directory *inside
the repository*, turning a hygiene failure into data loss. A broken binding raises the
new `SCRATCH_RETARGETED`, reported like every other hygiene failure: it never displaces
an earlier, more specific one.

**Regressions:** `test_a_scratch_retargeted_mid_run_is_refused_not_followed` performs the
reviewer's exact scenario (rename the parent, symlink it at the repo, recreate the
basename) and asserts every string conversion refuses, the descriptor-backed write still
lands outside, and cleanup deletes nothing inside the repo;
`test_a_scratch_whose_binding_holds_is_released_normally` proves the happy path still
releases.

### ARCH8-02 — the documented guarantee, narrowed to what is true

The doc now states the boundary exactly: *the gate guarantees that IT does not write into
the worktree, and refuses to run rather than write through a path it cannot vouch for; it
does not guarantee that nothing else did.* The write-then-delete measurement stays, since
it is why the second half cannot be promised. `test_every_diagnostic_code_the_gate_can_raise_is_documented`
failed until `SCRATCH_RETARGETED` was listed — caught before the gate ran, as designed.

---

## Stage-2 repo review — round 12 (RUN_DIR `/tmp/cdx-review.srlRcF`, `STATUS: completed`)

`SCOPE: branch diff against 1779e5b head=479546608414fc88a0a7aff0444b75ef9fe2756c dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX12-01 | "[P1] Validate the object opened for scratch use — `wave_gate.py:2031-2033`. When a same-user process retargets a user-controlled TMPDIR parent after `_refuse_scratch_inside_repo()` returns but before `os.open()`, the descriptor is opened on the replacement inside the repository. Subsequent stat/fstat checks agree because both name and descriptor reference that replacement… Acquire the descriptor first and validate that exact opened object and its containment." | P1 → **critical** | **fixed** |
| CDX12-02 | "[P1] Keep cleanup bound to the held scratch directory — `wave_gate.py:2077-2085`. …`__fspath__` returns a raw string, and `release()` closes the descriptor before `shutil.rmtree()` resolves it. A sibling can retarget the parent after the check, or move the same directory into the repository while preserving its inode, causing cleanup to delete through the worktree… Cleanup must remain descriptor-anchored and verify containment, not merely leaf identity." | P1 → **critical** | **fixed (mechanism) + scope corrected** |
| CDX12-03 | "[P2] Run the final fingerprint before reporting retargeting — `wave_gate.py:1988-1989`. When scratch retargeting is the only failure, this immediate raise bypasses the supposedly unconditional `check_worktree_unchanged()` below. That is precisely the path where repository mutation is most plausible, so the gate loses the `WORKTREE_DIRTY` evidence." | **standard** (blocking class: *runtime behavior*) | **fixed — and it turned out to be the load-bearing one** |

**CDX12-01.** Verified by reading: `_refuse_scratch_inside_repo(resolved, repo)` ran on the
PATH and `os.open()` followed it, so a swap between the two produced a descriptor on the
replacement — after which name and descriptor agree forever, because both denote the
replacement. The order is inverted: open first, judge the object we actually hold.
Containment now climbs `..` through descriptors rather than `os.path.dirname()`, because
`..` relative to a directory fd is the real parent in the real tree while `dirname()` is
just more string manipulation — the third and final form of the *decide-a-filesystem-fact-
by-string* class.

**CDX12-02.** The mechanism is fixed: `dispose()` unlinks contents relative to the held
descriptor (`_unlink_tree_at`), never by pathname, so a parent swapped mid-cleanup cannot
redirect the deletion; only the final `rmdir` of an *empty* directory goes by name, and
only while the binding holds. **The scope claim is corrected, not adopted**: "verify
containment, not merely leaf identity" cannot be satisfied, because a same-user process
can move the verified directory itself into the worktree, inode intact — no descriptor
anchoring prevents that. Chasing it would be another enumeration.

**What actually makes this sound is CDX12-03, which I had broken.** I wrote a block
labelled UNCONDITIONAL and then put `raise retargeted` above it, skipping it on exactly
the path where a repository mutation is most likely. Fixed by recording retargeting as a
PENDING failure. That closes the loop with `dispose()`'s refusal to delete: the write
SURVIVES, so the closing fingerprint sees it and reports `WORKTREE_DIRTY`. The guarantee
is therefore not "containment always holds" — it is **"if containment breaks, the gate
cannot hide it."** Three properties in combination: retargeting is a failure; nothing is
deleted through a broken binding; the fingerprint always runs. Pinned end-to-end by
`test_a_retarget_only_failure_still_reaches_the_closing_fingerprint`, which writes into
the retargeted directory and asserts `WORKTREE_DIRTY` still comes out.

The doc now states that invariant, and states plainly that containment is NOT stable.

## Stage-2 repo review — round 13 (RUN_DIR `/tmp/cdx-review.2ltTPL`, `STATUS: completed`)

`SCOPE: branch diff against 4795466 head=0a40279ec64b8612a90a253c7fb51ea4a4696628 dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX13-01 | "[P1] Preserve evidence if the scratch moves during disposal — `wave_gate.py:2130-2136`. …the binding is checked only once before destructive cleanup. If another process renames the held scratch directory into the worktree after `__fspath__()` returns but before `_unlink_tree_at()` scans it, fd-relative cleanup deletes the generated files from their new in-repo location; `os.rmdir(path)` then fails on the stale name, but that error is swallowed and `dispose()` returns `True`. Git ignores the empty directory left behind, so the closing fingerprint can match and the gate can exit 0, contrary to the new guarantee that retargeting evidence is never hidden." | P1 → **critical** | **fixed** |

Verified by reading my own code: `except OSError: pass` around `os.rmdir(path)` followed
by `return True`. A failing `rmdir` is the *signal* that the name stopped denoting our
directory, and I was discarding it — the gate would then exit 0 over a retargeting it had
already detected. That is a fail-open in the very rule introduced to prevent one.

**Fix.** The destructive step is now BRACKETED by identity+containment checks instead of
merely preceded by one, and `_binding_holds()` checks both (the containment half catches
the held directory being *moved* into the repo, which identity alone cannot see):

- check → if the directory is already inside the worktree, delete **nothing**; this is
  the ordinary version of the race, and the generated files survive intact;
- unlink contents fd-relative;
- check again → a move that lands mid-delete cannot un-delete the files, but it does
  make the run RED rather than green;
- `rmdir` failure ⇒ `False`, never a shrug.

**Regressions:** `test_a_scratch_moved_into_the_repo_is_not_emptied` renames the held
directory into the repo and asserts the generated file is still readable afterwards;
`test_a_failing_rmdir_is_a_broken_binding_not_a_shrug` forces `rmdir` to fail and asserts
`dispose()` reports `False`.

Note the guarantee's exact shape, unchanged since round 12: the gate does not promise the
bytes survive every possible interleaving — it promises it never **passes** over one.

## CHECKPOINT — Stage-2 repo review loop, evaluation 12 (recorded at 13→14 boundary)

- **Loop identity:** Stage-2 repo Codex review, slice #152. Window 4, cumulative 13.
- **Validation owed and completed first:** wave gate exit 0 at each tip (9723 passed /
  18 skipped, 60 goldens deterministic), darkness proof, delta-scoped reviews.
- **Per-tier:** critical 0 unresolved (all fixed and validated); standard 0 unresolved.
- **Defect classes:** *check-to-use race in scratch handling* — **recurring**, r12-01
  (validate-then-open) and r13-01 (check-then-delete) are the same class.
- **Trend:** findings 3 → 1; P1 count 2 → 1; each round strictly narrower (whole
  approach → one race → one swallowed error). Improving on every axis, none worsening.

**The recurrence triggers the structural-fix rule, not a third instance patch.** Both
races have the same shape: a destructive operation that resolves a mutable *name*. The
invariant that dominates the whole class is therefore *no destructive operation resolves
a pathname at all*. `_unlink_tree_at` was already descriptor-relative; the one remaining
path-based destructive call was the final `os.rmdir(path)`. `make_scratch_dir` now
captures the REAL parent at creation (`os.open("..", dir_fd=fd)` — the real parent
whatever the name says) and disposal ends with
`os.rmdir(basename, dir_fd=parent_fd)`. After this, no destructive operation in this
class can be redirected by renaming anything, so there is no next site of this shape to
find.

**OUTCOME: `CONTINUE`** — new window. Legal on the recorded trend (nothing worsening,
several axes materially better) with a named finite next action, which is the structural
fix above, already applied. Zero critical residue at the tip.

### Harness defect caught by controlling the harness

Applying the structural fix changed `rmdir`'s call shape from `rmdir(fullpath)` to
`rmdir(basename, dir_fd=…)`. `test_a_failing_rmdir_is_a_broken_binding_not_a_shrug`
matched on the full path, so its forced failure **stopped firing** — and the test kept
passing, now exercising nothing. Measured rather than assumed:
`rmdir called with: [('wave-gate-2d_y63iw', 4)]`, `dispose -> True`.

Repaired to match basename + `dir_fd`, plus an explicit
`assert seen, "the forced failure never fired — the test would be vacuous"` so the same
drift cannot recur silently. Then proven to bite: with the forced failure firing,
`dispose with failing rmdir -> False`. A green test whose target changed shape is not
evidence — this is the [[verify-claims-and-harnesses-by-measuring]] failure mode, caught
here only because the fix's *call shape* changed and I went looking.

## Stage-2 repo review — round 14 (RUN_DIR `/tmp/cdx-review.z8zupW`, `STATUS: completed`)

`SCOPE: branch diff against 6176ff9 head=417c4aec7ab9afa40cf7333634d6a24de315d18b dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX14-01 | "[P2] Bind rmdir to the scratch's current parent — `wave_gate.py:2166`. When another same-user process renames the original parent, recreates its old pathname, and moves the held scratch back under that recreated parent, both `_binding_holds()` checks pass because `_path` still names `fd`, but `_parent_fd` still names the old directory. If that old directory has an empty entry with the same basename, this call removes that unrelated entry, leaves the real scratch directory in place, and returns `True`; capture or verify the current parent-child binding during disposal instead of caching it at construction." | source P2 → **standard** (blocking class: *runtime behavior*) | **fixed** |

**Reproduced before fixing**, against the code as committed at `417c4ae`:

```
binding still holds : True
OLD cached-parent rmdir removed the DECOY: True
OLD left the real scratch behind        : True
```

The cached handle is genuinely stale, and both `_binding_holds()` checks pass while it
is — identity and containment are properties of the scratch, and say nothing about a
parent captured earlier.

**Fix — delete the state rather than add a check.** The reviewer offered "capture or
verify"; capturing at disposal is strictly better than verifying a cached value, because
`..` from the held descriptor IS the current parent by construction, so there is no
window in which the two can disagree and no stale field to reason about. `_parent_fd`
and `_close_parent()` are gone entirely; the parent is opened immediately before `rmdir`
and closed after. Note this is the fix from the round-12 checkpoint *corrected*, not
abandoned: the destructive call is still descriptor-relative and still resolves no
pathname — it just stops carrying state it does not need.

**Regression:** `test_disposal_uses_the_scratchs_current_parent_not_a_cached_one` builds
the exact scenario (rename the parent away, recreate its pathname, move the scratch back
under the recreation, leave a same-named decoy behind) and asserts the real scratch is
removed and the decoy untouched. Proven to bite: under the old cached-parent logic the
decoy is removed and the scratch survives; under the new logic
`NEW removed the real scratch: True`, `NEW left the decoy alone: True`.

## Stage-2 repo review — round 15 (RUN_DIR `/tmp/cdx-review.A1dzXt`, `STATUS: completed`)

`SCOPE: branch diff against 417c4ae head=9796d0c076fa098a155085d519d60ffb8a43fd75 dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX15-01 | "[P1] Verify the live parent/name pair before rmdir — `wave_gate.py:2168`. When the held scratch is moved to `Q/new-name`, its original path is replaced with a symlink to it, and an empty `Q/<original-name>` exists, both `_binding_holds()` calls still pass because `os.stat` follows the symlink. The new `open(\"..\")` returns `Q`, but this line removes `<original-name>`, deleting the unrelated directory, leaving the real scratch behind, and returning `True`; require a no-follow child binding or otherwise prove the parent/name pair identifies the held inode before removal." | source P1 → **standard** (blocking class: *runtime behavior*; no critical anchor — `rmdir` removes only an EMPTY directory, outside the repository, so no data is lost and the gate's verdict about the worktree is unaffected) | **fixed** |

**Reproduced before fixing:**

```
binding holds                  : True
OLD deleted the UNRELATED dir  : True
OLD left the real scratch      : True
```

Correct: I fixed the stale *parent* in round 14 and left the stale *name*. Deriving one
half of the pair live and trusting the other half from construction proves nothing about
the pair.

**Fix.** Disposal no longer trusts a remembered basename. `_entry_naming(parent_fd, held)`
scans the live parent for the entry whose `lstat` inode IS the held directory —
`follow_symlinks=False`, so a symlink *pointing at* the directory is correctly not the
directory — and `rmdir` removes that name. The stored `_path` is now used only for the
read-side identity check, never to choose what to destroy.

**Regression:** `test_disposal_removes_the_entry_that_is_the_scratch_not_its_old_name`
builds the reviewer's scenario exactly and asserts the real scratch is removed and the
unrelated directory survives. Proven to bite: `NEW removed the real scratch: True`,
`NEW left the unrelated dir: True`, against `OLD deleted the UNRELATED dir: True`.

**Residual, stated rather than hidden:** a POSIX `rmdir` cannot be made atomic with the
inode check preceding it. The residual window can only remove an EMPTY directory, so it
loses no data, and it lies outside the repository, so it cannot change the gate's verdict.

## Stage-2 repo review — round 16 (RUN_DIR `/tmp/cdx-review.h2bjz0`, `STATUS: completed`)

`SCOPE: branch diff against 9796d0c head=5e722f52333ed7e46c4022b651e8c179adfa5a98 dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX16-01 | "[P2] Keep rmdir bound to the verified inode — `wave_gate.py:2181`. `_entry_naming()` proves the inode only while scanning; …a sibling can move the now-empty scratch after the scan and install an unrelated empty directory at `name` before this call. `rmdir` then removes the replacement and returns success while the held scratch survives—potentially inside the repository, where the fingerprint ignores empty directories. Use an identity-bound removal mechanism, or fail closed rather than deleting through a non-atomic scanned name." | source P2 → **standard** (blocking class: *runtime behavior*) | **fixed — by the reviewer's second option** |

This is the residual I recorded at round 15 rather than a new discovery, and the reviewer
is right that recording it is not the same as handling it.

**The first option offered does not exist.** POSIX has no remove-by-descriptor: there is
no `frmdir(fd)`. So no guard placed BEFORE `rmdir` can be atomic with it, and rounds
12→16 are the evidence — each pre-check I added closed one window and revealed the next.
Adding a sixth would be the enumeration this project's structural-fix rule forbids.

**The second option does exist and terminates the class.** The gate now asserts the
OUTCOME instead of guarding the attempt: after `rmdir`, `_removal_proved()` requires that
the held directory is no longer listed in the parent it removed from, and that `..` from
the held descriptor still names that parent (catching a scratch moved elsewhere mid-race).
Whatever the interleaving, the gate either proves it removed the directory it held, or
fails closed. There is no next window to find, because the check is on the result rather
than on a precondition.

**Both discriminators were MEASURED, and the obvious one is wrong here.** My first
instinct was `st_nlink == 0` after removal. On macOS/APFS that is false:

```
nlink while it exists      : 2
nlink after it was removed : 2
nlink when the WRONG dir was removed: 2
```

A link-count test would have agreed with every case and silently proved nothing — on the
platform I develop on. What does discriminate, measured:

```
A) parent still lists our inode after removal: None        # correct removal
C) parent lists a LIVE inode                 : other        # removed something else
B) '..' of a REMOVED dir -> opened, same as parent: True
```

**Regression:** `test_disposal_reports_failure_when_the_removal_hit_the_wrong_directory`
fires the race inside `os.rmdir` itself — moving the emptied scratch aside and installing
an unrelated empty directory at the scanned name in exactly the described window — and
asserts `dispose()` returns False, with `assert fired` so the test cannot pass vacuously
if the window ever moves.

## Stage-2 repo review — round 17 (RUN_DIR `/tmp/cdx-review.4yOySS`, `STATUS: completed`)

`SCOPE: branch diff against 5e722f5 head=c301c610156bd9ff1b40a8975f161c8ca2f451db dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX17-01 | "[P2] Fail closed when the parent probe errors — `wave_gate.py:2248-2251`. When the held scratch is still linked but `os.open(\"..\", dir_fd=fd)` fails… this branch returns `True`… inability to inspect `..` is not proof of unlinking and must be treated as unproven." | **standard** (runtime behavior) | **fixed** |
| CDX17-02 | "[P2] Avoid racing the two outcome observations — `wave_gate.py:2246-2253`. …a sibling can keep the held scratch under another parent until `_entry_naming()` returns `None`, then rename it back under `parent_fd` before `os.open(\"..\")`. Both checks then pass and the function returns `True`…" | **standard** (runtime behavior) | **deferred → #164** |
| CDX17-03 | "[P2] Preserve the fingerprint when removal verification errors — `wave_gate.py:2187-2189`. If the new post-removal scan or `fstat()` raises an `OSError`, it escapes `_removal_proved()` before either descriptor is closed. Because `dispose()` runs in `execute()`'s `finally`, this also suppresses any prior `GateFailure` and skips `check_worktree_unchanged()`, producing an uncoded traceback rather than the required fail-closed diagnostic." | **standard** (runtime behavior) | **fixed structurally** |

**CDX17-03 is the third appearance of one class** — *an exception escaping the cleanup
path destroys the gate's reporting* (previously the unrenderable diagnostic, then
CDX12-03's early `raise`). Per the structural-fix rule the guarantee is now placed at the
boundary rather than chased call by call: `dispose()` is **total by construction** — its
whole body is guarded, no path out of it raises, descriptors close in `finally`, and
anything unexpected reads as "not disposed", the fail-closed answer. Pinned by
`test_dispose_never_raises_even_when_the_filesystem_does`.

**CDX17-01, and the assumption I nearly shipped.** I had written `except OSError: return
True` reasoning "not linked anywhere ⇒ gone". The reviewer is right that an unreadable
`..` is not proof. But simply inverting it to `return False` would have been just as
unfounded in the other direction: if a platform invalidates `..` after removal, every
ordinary successful disposal would report failure and the gate would be permanently red
on the deployment interpreter — which I could not test from here. So the platform's
behaviour is **measured at runtime**, once per run, inside the scratch directory that has
just passed containment (`_probe_dotdot_at`), using a directory the gate creates and
removes itself. `None` (probe could not run) fails closed. Pinned by
`test_an_unreadable_parent_probe_is_not_proof_of_removal`, which asserts the probe
produced a definite answer before relying on it.

## CHECKPOINT — Stage-2 repo review loop, evaluation 17

- **Loop identity:** Stage-2 repo Codex review, slice #152. Window 2 of the post-r12
  window; cumulative 17.
- **Validation completed BEFORE this decision:** wave gate exit 0 (9726 passed / 18
  skipped, 60 goldens deterministic), darkness proof, gate suite 265 → 267.
- **Per-tier:** critical **0** unresolved. Standard: 2 fixed, **1 deferred to #164**.
- **Defect classes:** *exception escaping cleanup* — recurring ×3, closed structurally by
  making `dispose()` total. *TOCTOU in scratch disposal* — the irreducible remainder.
- **Trend across the class:** impact strictly and materially decreasing —
  r12/r13 could corrupt the gate's verdict (**critical**); r14/r15 could delete an
  unrelated **empty** directory outside the repo; r16/r17 can at worst leave an **empty
  untracked** directory inside it, which git does not track. Findings per round 3 → 1 → 1
  → 1 → 3, but severity anchors moved critical → standard and stayed there.

**OUTCOME: `DEFER-STANDARD-AND-PROCEED`.** Legal here: this is not the last owed loop
(§6 is still owed), there is **zero critical residue** in this loop, the single deferred
finding is individually enumerated in an **already-filed** issue (**#164**, acceptance
criteria recorded in its body), and current-tip validation is complete. The deferral is
carried to the final bar.

Rationale for deferring rather than continuing: the remaining item cannot be closed by
another check. POSIX provides no atomic remove-by-descriptor and no atomic multi-namespace
observation, so rounds 12→17 have been closing one window and revealing the next — the
workflow's own guidance that *"after a dozen small fixes, more small findings mean the
next issue matters more"* is exactly this situation. #164 records the mechanism change
required, and the bounded impact is stated in `ENDGAME_VERIFICATION_GATE.md` rather than
left implicit.

## Stage-2 repo review — round 18 (RUN_DIR `/tmp/cdx-review.mjAM8c`, `STATUS: completed`)

`SCOPE: branch diff against c301c61 head=58d6ed0d83de63e98ad58830c365bcfdb707ecd7 dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX18-01 | "[P1] Match the calibrated `..` error before accepting removal — `wave_gate.py:2300-2302`. On filesystems where `_probe_dotdot_at` returns `False`… this accepts every later `OSError`, including `EACCES` or `EMFILE`, as the unlink signal… preserve and compare the calibrated errno, otherwise fail closed. **The new test misses this because the still-linked scratch is found by `_entry_naming` before its `blind` hook runs.**" | **standard** (runtime behavior) | **fixed** |
| CDX18-02 | "[P2] Guard the final close inside the no-throw boundary — `wave_gate.py:2165-2166`. If a non-`OSError` is raised while `self._close()` runs… this `finally` executes outside the `except BaseException`, so the exception still escapes… include the close operation in the guarded verdict and return false on its failure." | **standard** (runtime behavior) | **fixed** |

Both reproduced before fixing:

```
ESCAPED the 'total' boundary: RuntimeError close blew up
entry_naming finds it first  -> wave-gate-0bo5omvy
so the '..' branch is NEVER reached in that test
```

**CDX18-02 — my "total by construction" boundary was not total.** I placed `finally:
self._close()` *outside* `except BaseException`, and a `finally` runs after the handler
completes, so anything it raises escapes regardless. The close now sits inside the guard.
This is the FOURTH appearance of *escape from the cleanup path* — and notably the third
fix for it was itself wrong, which is the argument for testing the guarantee rather than
inspecting it: `test_dispose_is_total_even_when_closing_the_descriptor_throws` drives a
raising `os.close` and asserts a verdict comes back.

**CDX18-01 — the calibration was too coarse.** `_probe_dotdot_at` returned a bare
`False`, so `_removal_proved` accepted ANY `OSError` as the unlink signal on such a
platform; an unrelated `EACCES`/`EMFILE` would have been read as "cleanly disposed". The
probe now returns the SPECIFIC `errno`, and proof requires an exact match.

**The reviewer also caught my test passing for the wrong reason**, which I confirmed
directly: with the scratch still linked, `_entry_naming` returns a name and
`_removal_proved` decides at the first branch — the `..` code never executed. The
assertion was true and worthless. Rewritten to stub the listing so the branch under test
actually runs, with `assert reached` guarding vacuity, and it now covers all four
calibration cases including the discriminating one (calibrated `ENOENT` vs. a raised
`EACCES` ⇒ unproven; calibrated `EACCES` vs. raised `EACCES` ⇒ proven).

*Third vacuous-harness incident in this slice* (after the `rmdir` call-shape drift and
this one's first draft). All three were caught by measuring what the test executed rather
than trusting that it was green — [[verify-claims-and-harnesses-by-measuring]]. Every new
negative test in this area now carries an explicit `assert <hook> fired` line.

## Stage-2 repo review — round 19 (RUN_DIR `/tmp/cdx-review.hHwRBN`, `STATUS: completed`)

`SCOPE: branch diff against 58d6ed0 head=37b32ecb0649dbef0ce3af3c276cd1673c979ed6 dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX19-01 | "[P1] Exclude booleans from calibrated errno matches — `wave_gate.py:2319`. On filesystems where `_probe_dotdot_at()` records `True`, `True` passes `isinstance(..., int)` and equals `errno.EPERM` (`1`). If the post-`rmdir` parent lookup then fails with EPERM, this branch returns success even though no failure errno was calibrated… Require an actual integer type or a tagged calibration result before comparing." | **standard** (runtime behavior) | **fixed** |

**Not the deferred class.** This is not another interleaving variant of #164 — it is a
plain type-confusion defect in the discriminator itself, and it fails OPEN. Per the
disposition I stated for this loop (defer further same-class races; fix anything genuinely
new or fail-open), it gets fixed. Confirmed by measurement:

```
isinstance(True, int) : True
True == errno.EPERM   : True
type(True) is int     : False
```

**Fix.** The calibration is now a TAG (`_DOTDOT_SURVIVES = object()`), not a bool: the two
outcomes are different kinds of answer and no longer share a type. The errno comparison
uses `type(x) is int`, which excludes `bool`, rather than `isinstance`. I introduced this
overload myself one round earlier by widening the probe's return value from a bool to
"bool-or-errno" without changing how it is discriminated — widening a return type without
revisiting every test of it is its own small defect class.

**Regression:** `test_a_survives_calibration_is_never_matched_as_an_errno` raises `EPERM`
from the parent lookup under a survives-calibration and asserts the result is `False`,
plus the same for a literal `True`, with `assert reached` so it cannot pass vacuously.

---

## §6 architect review — round 9 (RUN_DIR `/tmp/cdx-gate-review.TnpZpj`) — **GATE FAILED, NOT ATTESTED**

`gate-attest collect` refused: `{"ok":false,"reason":"turn_not_completed","stopped":true}`.
The daemon reported `turnStatus: failed` on the retry turn even though it produced a
complete review with a valid verdict line. **No salvage file was written**, so there is no
attested artifact and no `review.md`. Per the §6 contract the decision comes from the
collector's JSON, not the helper's report — so **this round does not count as a §6
evaluation** and the gate is still owed. Its findings are nevertheless treated as
evidence, because a finding's truth does not depend on its attestation, and two of them
were reproducible data-loss defects.

Round 9 must be RE-RUN for attestation before the slice can close.

### Findings acted on (verified independently against the tree, not taken on trust)

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| ARCH9-01 | "Critical: the opened directory is checked for containment but never proven to be the directory `mkdtemp()` created… the gate accepted it, wrote into it, and `dispose()` recursively deleted it" | **CRITICAL** (anchor: *data loss*) | **fixed** |
| ARCH9-02 | "Critical: constructor-failure cleanup returns to `shutil.rmtree(candidate)` through the mutable pathname… `rmtree(candidate)` deleted the tracked subtree… and the opened descriptor is leaked" | **CRITICAL** (anchor: *data loss*) | **fixed** |
| ARCH9-03 | "`execute()` catches only `GateFailure`; an ordinary `RuntimeError`/`OSError`… propagates before both `SCRATCH_RETARGETED` bookkeeping and the closing fingerprint" | standard (runtime behavior) | **fixed** |
| ARCH9-04 | "the cleanup description overstates the mid-disposal guarantee: `_unlink_tree_at()` runs between the two binding checks" | standard (served contract text) | **fixed (doc)** |
| ARCH9-05 | "the two golden passes use separate processes but the SAME scratch directory, contrary to the required separate temporary directories… this can change the wave verdict" | standard (runtime behavior) | **fixed** |
| ARCH9-06 | "local `--bootstrap` is an unchecked operator assertion rather than the architect's immediate-introduction exception" | standard | **finding-refuted** |
| ARCH9-07 | "the ancestry proof uses path-limited `git log` without `--full-history`, so an addition on a side branch discarded by an `ours` merge is pruned" | standard (runtime behavior) | **fixed** |
| ARCH9-08 | "newly appended tombstones are deliberately accepted, contrary to the plan's committed negative case… the current bootstrap already contains 15 such tombstones and is no longer the required all-active, lexicographically sorted final bootstrap" | **CRITICAL** (anchor: *mutation accounting*) | **fixed (code + data)** |
| ARCH9-09 | "`try_catch_dlq_error_subprocess.xml` is recorded `owner:\"repository\", disposition:\"survivor\"` instead of the fixed `owner:\"#160\", disposition:\"deletion_only\"`; because these fields are immutable, #160 cannot correct them later" | **CRITICAL** (anchor: *mutation accounting* — an immutable ledger row) | **fixed** |
| ARCH9-10 | "`schema_version` is compared by equality without an exact integer type check, so JSON `true` and `1.0` are accepted as version 1" | standard (runtime behavior) | **fixed** |
| ARCH9-11 | "symlink rejection checks only the final file, not ancestor directories" | standard (runtime behavior) | **fixed** |
| ARCH9-12 | "the required `BOOMI_LOCAL=true`, `BOOMI_DOCS_ENABLED=false`, `BOOMI_GOTCHAS_ENABLED=false` environment is absent… CI is order-dependent" | standard (runtime behavior) | **fixed** |
| ARCH9-13 | "`ci` never invokes the #153 seam, and normal `wave` emits lowercase human text rather than `PLAN_FINGERPRINT_PENDING issue=#153`" | standard (machine-served contract) | **fixed** |
| ARCH9-14 | "argument parsing occurs outside the coded failure boundary: passing `--base` to `ci` exits 2 with argparse usage text and no stable diagnostic code" | standard (machine-served contract) | **fixed** |
| ARCH9-15 | "dependency direction is inverted: the registry imports producer test modules… the required authority refactor was not delivered" | standard | **deferred → #165** |
| ARCH9-16 | "rollout remains explicitly incomplete: no recorded Python 3.11 Actions result, no seeded red/revert-green evidence, no required `dev` ruleset" | not a defect | **not-validated — this IS the §8 finish**, whose push produces the Actions evidence; the doc's status banner already states it |

### The two criticals, reproduced before and after

```
=== CRITICAL 1: swap an unrelated directory onto the candidate name ===
  refused: SCRATCH_NOT_OURS
  precious.txt still present: True
=== CRITICAL 2: move a TRACKED dir onto the candidate name on the failure path ===
  refused: SCRATCH_INSIDE_REPO
  the tracked subtree still exists: True
```

ARCH9-01: containment answers "is this outside the repo", never "is this the directory I
made" — and an attacker's directory outside the repo passes containment perfectly.
`_refuse_scratch_created_here` now requires the inode observed immediately after
`mkdtemp`, mode 0700, our uid, **and emptiness** — the last is what defeats the
swapped-directory-full-of-real-files case even if the identity observation were raced.

ARCH9-02: the failure path did `shutil.rmtree(candidate)` — through the very pathname the
gate had just decided to distrust — and leaked the descriptor. Now discarded through the
descriptor via `_discard_scratch_at`, which can only name the directory actually opened.

### ARCH9-06 — refuted, with the code as evidence

`check_bootstrap` DERIVES the exception regardless of the flag: it reads whether both
manifests exist at the baseline (`_blob_at`), returns False when they do, refuses a
half-introduction, and requires the ancestry proof (`neither path ever touched in the
baseline's history`) plus the target proof. `require_flag` only ADDS a local confirmation
(`if require_flag: if not flag_given: raise`) — it can never substitute for the
derivation. What the reviewer's reproduction demonstrates is the consequence of an
operator-chosen `--base` pointing before the manifests landed, where bootstrap genuinely
applies because there is no prior manifest to transition from. That is the local trust
boundary this slice already established and which the §6 gate itself accepted at round 5
as "a legitimate structural resolution"; CI cannot reach it because the push's `before`
is the authority.

### ARCH9-08 — the plan is unambiguous, and my earlier decision contradicted it

Plan line 462: *"Appending a pre-tombstoned row is illegal."* Line 522:
*"| State | Append a tombstoned row → fail |"*. I had deliberately ALLOWED born-tombstoned
rows so that a multi-commit push adding and removing a test stayed legal. That solved a
problem which does not exist: from the range's endpoints such a test simply never existed,
so it needs **no row at all**. The allowance also opened a way to mint permanently
reserved identities with unchanged floors. Now refused as `MANIFEST_TRANSITION_ILLEGAL`.

The 15 tombstones in the manifest were `test_wave_gate.py` tests I added and removed
DURING this slice — pre-landing churn, not repository history. Since the manifest has not
landed, the bootstrap was regenerated per plan line 206: 9749 rows, **all active**,
lexicographically sorted by node id, ids sequential from `pytest-000001`. Verified:
`sorted: True`, `ids sequential from 1: True`, `all active: True`.

### ARCH9-15 — deferred to #165, and the measurement that changed the decision

My own implementation plan said *"Do not import test modules from the registry"* on an
estimate of ~4.9 s import cost per child process. Measured during implementation: ~0.39 s.
The constraint behind the instruction did not exist, and importing the producers achieves
the plan's stated GOAL (exactly one case definition) better than the alternative the plan
sketched, which duplicated ~27 literal configs. What it does not achieve is the plan's
authority DIRECTION, and that has a real cost the reviewer named: #159's
`transitional_oracle` and #160's `deletion_only` dispositions exist precisely to keep
goldens executing after their owning tests are removed — the removal they are designed to
survive is the one that breaks this direction. Enumerated in **#165** with acceptance
criteria; recorded in `ENDGAME_VERIFICATION_GATE.md`.

## Stage-2 repo review — round 21 (RUN_DIR `/tmp/cdx-review.CUCx7t`, `STATUS: completed`)

`SCOPE: branch diff against 164d9b3 head=38011027f9fa2de4808229a56619f9c151b8828e dirty=false`

Five findings. Two of them demand the OPPOSITE of what the architect gate demanded at
round 9, so the disagreement was settled by measuring which baseline the landing event
actually uses — not by preferring a reviewer.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX21-01 | "[P1] Preserve the append-only test-node ledger — `test_nodes.jsonl:9349`. Against the supplied base, this repoints `pytest-009348`… and the regenerated tail drops historical rows entirely (9,763 rows become 9,749). `validate_transition()` therefore exits 2 with `MANIFEST_TRANSITION_ILLEGAL`…" | P1 | **finding-refuted** |
| CDX21-02 | "[P1] Keep golden-000057 metadata immutable — `goldens.jsonl:58`. For every normal transition from this base, changing this row's `owner` and `disposition` is rejected… Restore `owner=\"repository\"` and `disposition=\"survivor\"`" | P1 | **finding-refuted** |
| CDX21-03 | "[P1] Keep render request writes descriptor-relative — `wave_gate.py:1543-1544`. When the scratch name is retargeted between `_render_dir()` converting it to a plain string and this `open()`, the write follows the replacement… the previous `open_for_write()` was descriptor-relative." | **standard** (runtime behavior) | **fixed — a regression I introduced** |
| CDX21-04 | "[P2] Put the coded usage error before argparse output — `wave_gate.py:2686`. `parse_args()` prints its usage and error text before raising `SystemExit`, so this catch emits `GATE_USAGE_INVALID` only afterward… violating the documented first-token machine contract." | **standard** (machine-served contract) | **fixed** |
| CDX21-05 | "[P2] Keep golden ancestor symlinks at validation status — `wave_gate.py:2456`. …the helper always constructs a status-2 contract failure. Every other golden-tree path for the same diagnostic uses `_invalid` and returns the documented validation status 1." | **standard** (machine-served contract) | **fixed** |

### CDX21-01 / CDX21-02 — refuted by measuring the landing baseline

Both findings scope to `164d9b3`, an intermediate commit ON THIS BRANCH. Nothing ever
validates that transition. Measured:

```
remote dev: 9080e3c2d0fcc82b01f781b2352d60995ba58ad8
  dev lacks tests/fixtures/wave_gate/test_nodes.jsonl
  dev lacks tests/fixtures/wave_gate/goldens.jsonl

=== the ACTUAL landing event: push to dev with before=9080e3c ===
wave_gate: baseline 9080e3c2d0fcc82b01f781b2352d60995ba58ad8 (push)
wave_gate: BOOTSTRAP — manifests are introduced by this change
wave_gate: manifests ok (9749 required nodes, 60 active goldens)
```

`dev` carries neither manifest, so the landing push takes the BOOTSTRAP path and
`validate_transition()` does not run. The branch's intermediate commits are not a
published ledger; the manifest that lands is the bootstrap, and the plan (line 206)
requires that bootstrap to be all-active and lexicographically sorted.

**CDX21-02 would be actively harmful if followed.** Restoring `owner="repository",
disposition="survivor"` would land a permanently wrong IMMUTABLE row — precisely the
outcome ARCH9-09 identified ("because these fields are immutable, #160 cannot correct
them later"). The two gates agree on the rule and disagree only on which baseline
applies; the measurement decides it.

*Recorded because the shape recurs:* two gates can each be right within their own frame.
The resolution is not to pick one but to establish which frame is real — here, by asking
git what `dev` actually contains.

### CDX21-03 — my own regression, from the fix two findings earlier

Splitting the render passes into separate directories (ARCH9-05) was correct, but I
implemented it by converting the pass directory to a plain string and calling `open()` —
discarding the descriptor-relative write that eight prior rounds had built. `_render_dir`
now returns `(path, fd)`, the request is written through the descriptor, and the pathname
is derived only for the child process, which must resolve a name because it is a separate
process. **A fix for one finding silently undid another.**

## Stage-2 repo review — round 22 (RUN_DIR `/tmp/cdx-review.Ms1i32`, `STATUS: completed`)

`SCOPE: branch diff against 3801102 head=38122e0ed82d072850732446ca49642027e3c9b5 dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX22-01 | "[P1] Preserve existing manifest IDs when adding tests — `test_nodes.jsonl:9448`. Against this comparison base, `pytest-009447` already names `test_a_symlinked_golden_is_refused`; inserting the new test here repoints that immutable ID and shifts 302 later IDs. Consequently, `manifests --base 3801102` exits 2 with `MANIFEST_TRANSITION_ILLEGAL`, **so the required CI and wave gates cannot run**." | P1 | **finding-refuted** |

Same class as CDX21-01, same grounds — the base is an intermediate commit on this branch,
and the landing event is a bootstrap from `9080e3c`, which carries no manifests.

**The finding's own consequence claim is falsified by direct measurement.** "The required
CI and wave gates cannot run" — both ran green on this exact tree:

```
HEAD 38122e0ed82d072850732446ca49642027e3c9b5
ci  (push, before=9080e3c): manifests ok (9752) · collection ok · suite green · exit 0
wave --base 9080e3c --bootstrap: 60 active goldens deterministic and byte-exact · exit 0
PLAN_FINGERPRINT_PENDING issue=#153
```

**One legitimate concern was buried in it, and is now addressed.** The append-only rule is
real and binds absolutely — but only once the bootstrap has LANDED. `ENDGAME_VERIFICATION_GATE.md`
§9 already mandated append-only regeneration; it did not state the boundary. It now does,
explicitly, including the diagnostic question ("what does `dev` actually contain") and the
note that two separate review rounds read an intermediate commit as a baseline and
reported a legal bootstrap as an illegal transition. Documenting the distinction is the
durable fix; refuting the finding twice without writing it down would invite a third.

---

## §6 architect review — round 9 RE-RUN (RUN_DIR `/tmp/cdx-gate-review.Eredut`) — **ATTESTED**

`{"ok":true,"gate":"review","status":"completed","turnToken":1,"parsedVerdict":"ISSUES FOUND"}`

The §6 gate is now spent for this round. The reviewer confirmed the closure of findings
2, 4 and 6–13 from the unattested pass, **and explicitly ratified the bootstrap
judgment**:

> "`dev` and `origin/dev` are `9080e3c`; neither manifest exists or was touched in that
> commit's full ancestry. The landing push therefore takes the bootstrap path.
> Intermediate feature-branch commits are not ledger transitions. The test ledger has
> 9,752 all-active, sorted, sequential rows… The golden ledger has 60 all-active, sorted,
> sequential rows: 57 survivors, two `#159/transitional_oracle`, and the correct
> `#160/deletion_only` row."

That settles the CDX21-01/02 and CDX22-01 refutations on the architect's own authority.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| ARCH9R-01 | "[Critical] Scratch cleanup can delete unrelated data and still pass. `_ScratchDir._dispose()` recursively deletes every entry through `_unlink_tree_at()`, without proving those entries were created by the gate. I moved an unrelated directory containing `precious.txt` into a valid scratch before disposal; `dispose()` returned `True`, the file was deleted, and the worktree fingerprint remained unchanged. **This is not #164**…" | **CRITICAL** (anchor: *data loss*) | **fixed** |
| ARCH9R-02 | "[P1] An unexpected `SystemExit(0)` still makes the gate green… `main()` only catches `GateFailure`, so `SystemExit(0)` escapes as process status 0. Reproduced through the real `main()`… Normalize unexpected exceptions to a status-1 `GateFailure`. Also move `make_scratch_dir()` inside the closing-fingerprint boundary." | **CRITICAL** (source P1; blocking class *runtime behavior* — a green gate over an unvalidated run) | **fixed** |
| ARCH9R-03 | "[P1] The two golden passes do not have isolated runtime filesystems… both children still run with the repository as `cwd` and the same inherited `TMPDIR`… A synthetic renderer whose seed-2 output differed but cached through `tempfile.gettempdir()` made both passes return seed-1 bytes, and `check_goldens()` accepted them. Thus round-9 finding 5 is not effectively closed." | **standard** (runtime behavior; can change the wave verdict) | **fixed** |
| ARCH9R-04 | "[P2] `bootstrap_base` still accepts the wrong JSON type… A JSON integer containing 40 decimal digits therefore parses successfully" | **standard** (machine-served contract) | **fixed** |
| ARCH9R-05 | "[P2] The durable documentation contradicts the landed contract" — four places + README | **standard** (served contract text) | **fixed** |

### ARCH9R-01 — reproduced, and the fix is ownership rather than enumeration

```
dispose() -> True
precious.txt deleted: True
```

A recursive sweep of "whatever is in the scratch" deletes anything a concurrent process
moved in. The gate now records an INVENTORY of what it creates (`open_for_write`,
`mkdir_owned`, `own`) and `_remove_owned` deletes exactly those, deepest first; anything
left over is refused as the new `SCRATCH_FOREIGN_ENTRIES` and nothing further is removed.
Hardcoding the filenames in the cleanup would have been the same list written twice —
recording ownership at the point of creation cannot drift from what is actually created.

### ARCH9R-02 — the fail-open class, one layer above where I had been looking

Round 17 made `dispose()` total and round 18 pulled its `close()` inside the guard. Both
were about exceptions escaping CLEANUP. This is the same class one layer up: `execute()`
recorded the unexpected exception and then **re-raised it**, handing the process that
exception's own exit semantics. `SystemExit(0)` therefore exited GREEN — a gate reporting
success for a run that never completed. Unexpected exceptions are now normalized to a
coded status-1 `GATE_UNEXPECTED_ERROR`, and the message inspects nothing about the object
(reading even `type(exc).__name__` can run foreign code through a metaclass hook — the
route four earlier rounds closed on the diagnostic path). `make_scratch_dir()` moved
inside the boundary as the reviewer asked. Pinned by
`test_an_unexpected_systemexit_cannot_make_the_gate_green`, with `assert fired`.

### ARCH9R-03 — my fix for ARCH9-05 was incomplete, not wrong

Separate request DIRECTORIES were not separate runtime FILESYSTEMS: both children kept
the repo as `cwd` and inherited the same `TMPDIR`, so a renderer caching through
`tempfile.gettempdir()` served pass 1's bytes to pass 2 and the determinism check agreed
with itself. Each child now gets `cwd` and `TMPDIR`/`TEMP`/`TMP` set to its own pass
directory; every path handed to the child is absolute, so nothing depends on `cwd`.

### A near-miss worth recording

Applying these fixes with a slice-based edit (`s[index(a):index(b)]`) silently deleted
**183 lines**, including four security-relevant helpers —
`_refuse_symlinked_ancestor`, `_refuse_scratch_created_here`, `_discard_scratch_at`,
`_refuse_scratch_inside_repo` — because the slice's end anchor had moved. The suite caught
it immediately (44 failures, `NameError`), and they were restored verbatim from HEAD. A
structural edit whose bounds are computed from moving anchors is not a safe edit; the
tests are what made it recoverable rather than a silent hole in the gate's defenses.

## Stage-2 repo review — round 23 (RUN_DIR `/tmp/cdx-review.YownB8`, `STATUS: completed`)

`SCOPE: branch diff against 1083413 head=12d4ab8b8100898e8c39baf589f66b9e788a771a dirty=false`

Both findings are about the ownership-inventory fix from the attested §6 round — the
fix's own new surface.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX23-01 | "[P1] Avoid following replacement symlinks during cleanup — `wave_gate.py:2691`. When a same-user process replaces `render-1` with a symlink before disposal, deleting the recorded path `render-1/request-1.json` follows that intermediate symlink. If the unrelated target has a file with that name, the gate deletes it and can then unlink the symlink and return success; the previous `follow_symlinks=False` walk only removed the symlink." | **CRITICAL** (anchor: *data loss*) | **fixed** |
| CDX23-02 | "[P2] Classify foreign files inside owned directories correctly — `wave_gate.py:2357-2360`. When an extra file is placed inside an owned `render-*` directory, its `rmdir` raises `ENOTEMPTY`, and this catch returns before the later `os.listdir(self.fd)` check. `refusal_code` therefore remains `SCRATCH_RETARGETED` instead of the documented `SCRATCH_FOREIGN_ENTRIES`" | **standard** (machine-served contract) | **fixed** |

**CDX23-01 is a regression my own fix introduced**, and it is worth naming precisely: the
inventory fix replaced a `follow_symlinks=False` recursive walk with `os.unlink()` on
MULTI-COMPONENT recorded names. A recorded path is only as safe as the components it
traverses — `unlink("render-1/request-1.json")` resolves `render-1` at call time and
follows a replacement. Removal now opens every component with `O_NOFOLLOW` from its
parent's descriptor (`_remove_owned_entry`), so a replaced component is refused rather
than traversed. This is the second time in this slice that fixing one finding silently
undid a property an earlier round had established (the first was the render-request write
losing `open_for_write`); both were caught by the next review rather than by me.

Reproduced against the fix:

```
--- A: owned dir replaced by a symlink to unrelated data ---
  dispose -> False code: SCRATCH_FOREIGN_ENTRIES
  unrelated file survived: True
--- B: foreign file NESTED inside an owned directory ---
  dispose -> False code: SCRATCH_FOREIGN_ENTRIES
```

**CDX23-02**: a nested foreign entry never reaches the top-level `listdir`, because its
parent's `rmdir` fails first with `ENOTEMPTY`. `_ForeignEntry` now carries that case to
the classifier, so the documented code is what machine consumers actually receive.

## Stage-2 repo review — round 24 (RUN_DIR `/tmp/cdx-review.YGicTy`, `STATUS: completed`)

`SCOPE: branch diff against 12d4ab8 head=f436d060c60a8b4d1b66f8b06ca98979c53391c6 dirty=false`

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX24-01 | "[P1] Append new test nodes instead of repointing immutable IDs — `test_nodes.jsonl:9361`. Against the specified base, `validate_transition` sees `pytest-009360` change… Consequently, `manifests`, `ci`, and `wave` stop with `MANIFEST_TRANSITION_ILLEGAL` before testing." | P1 | **finding-refuted (third occurrence)** |
| CDX24-02 | "[P1] Revalidate child bindings around descriptor-relative deletion — `wave_gate.py:2718`. …a same-user sibling can rename an opened `render-*` directory into the worktree after `os.open` but before this recursive call… the resulting empty in-repo directory is invisible to Git, allowing `dispose()` to return `True`. Re-prove each child remains named by its parent before and after recursive deletion, as is done for the scratch root." | **standard** (runtime behavior) | **fixed (described window) + remainder enumerated in #164** |

**CDX24-01 is the third report of the same refuted claim**, and it is now settled by the
ATTESTED architect gate rather than by my own argument. That gate's ratification, verbatim:
"neither manifest exists or was touched in that commit's full ancestry. The landing push
therefore takes the bootstrap path. Intermediate feature-branch commits are not ledger
transitions." The doc records the distinction, and the diagnostic question is written
down: **what does `dev` actually contain**.

**CDX24-02 — fixed by applying an existing proof uniformly, not by inventing another.**
The scratch root already re-proves its binding around destructive work; owned CHILD
directories did not. `_refuse_unbound_child` now makes the same `(st_dev, st_ino)`
assertion before and after each recursion. This is a missing application of the
established invariant rather than a new patch, which is why it does not fall foul of the
structural-fix rule that governs this class.

**The bound was measured, and it is the same as #164's:**

```
A) a NON-empty directory moved into the repo : WORKTREE_DIRTY -> gate red
B) an EMPTY directory moved into the repo    : BLIND (git does not track empty dirs)
C) the child-binding proof on a renamed child: _ForeignEntry raised -> refused
```

Content cannot be smuggled past the gate — anything containing a file is caught by the
closing fingerprint. Only an empty untracked directory is invisible. That bound is now a
TESTED property (`test_content_moved_into_the_worktree_is_caught_by_the_fingerprint`),
not a claim in prose, and the second site is enumerated in **#164's body** with an added
acceptance criterion requiring a fix to cover both sites.

Any further interleaving variant of this class goes to #164 rather than another round —
the disposition recorded at the evaluation-17 checkpoint, now applied.

## Stage-2 repo review — round 25 (RUN_DIR `/tmp/cdx-review.0rmwOK`, `STATUS: completed`)

`SCOPE: branch diff against f436d06 head=96ead47f67de3fa38f739752685f6181b584e0ea dirty=false`

ONE finding, the manifest-transition claim for the **fourth** time — no new issues, so the
delta is otherwise clean.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| CDX25-01 | "[P1] Append new nodes instead of repointing manifest IDs… the required `wave_gate.py ci` run deterministically raises `MANIFEST_TRANSITION_ILLEGAL` before the suite runs." | P1 | **finding-refuted (fourth occurrence)** |

Its stated consequence is falsifiable and was falsified on the same tip:

```
ci (push, before=9080e3c): baseline (push) · BOOTSTRAP · manifests ok (9759)
                           · collection ok · 9741 passed / 18 skipped · exit 0
```

Why this one is refused rather than conceded to repetition: complying would repoint the
bootstrap into a non-sorted ledger AND restore a permanently wrong immutable
`owner`/`disposition` on the #160 golden — a row that, once landed, #160 could never
correct. Four reports of the same misframing do not make it right, and the cost of
yielding is irreversible.

---

## §6 architect review — round 10 (RUN_DIR `/tmp/cdx-gate-review.kkeLZP`) — **ATTESTED**

`{"ok":true,"gate":"review","status":"completed","turnToken":2,"parsedVerdict":"ISSUES FOUND"}`

Confirmed closed by the reviewer: golden-child isolation, strict `bootstrap_base` typing,
README token. Three findings remained.

| Source ID | Verbatim summary (abridged) | Tier | Disposition |
| --- | --- | --- | --- |
| ARCH10-01 | "[high] ownership is recorded without exclusive creation: `O_CREAT \| O_TRUNC` lacks `O_EXCL`/`O_NOFOLLOW`. Reproduced — a foreign `collected.txt` was overwritten, then `dispose()` returned `True` and deleted it; the worktree fingerprint stayed unchanged… ownership was never established." | **CRITICAL** (anchor: *data loss*) | **fixed** |
| ARCH10-02 | "[P1] the closing `_status(repo)` fingerprint remains outside the `BaseException` normalization. Reproduced by making that second call raise `SystemExit(0)` after validation; it escaped `main()` with process status 0 (fail-open exit)." | **CRITICAL** (blocking class *runtime behavior*: a green gate over an unvalidated run) | **fixed** |
| ARCH10-03 | "[P2] the section still says the `BOOMI_*` variables are absent/dropped, contradicting the workflow… lines 271-280 also still describe the removed `_unlink_tree_at()` cleanup." | **standard** (served contract text) | **fixed** |

**ARCH10-01 — I had recorded ownership rather than establishing it.** The inventory fix
assumed that appending a name to `_owned` made the entry ours; `O_CREAT|O_TRUNC` does not
create, it TAKES OVER. Reproduced before the fix:

```
open_for_write ACCEPTED and truncated the foreign file
content now: gate output
dispose -> True (deletes what it never owned)
```

Creation is now the PROOF: `O_EXCL|O_NOFOLLOW`, and the record is appended only after the
exclusive create succeeds. Verified after:

```
refused: FileExistsError — ownership not established
foreign content intact: True
dispose -> False code: SCRATCH_FOREIGN_ENTRIES
```

**ARCH10-02 — the fail-open class, at the very last step.** Rounds 17, 18 and the attested
round 9 each closed this class one layer further out (dispose totality, its `close()`, then
`execute()`'s re-raise). The closing `check_worktree_unchanged(status_before, _status(repo))`
was still outside the boundary — so a `SystemExit(0)` from the FINAL call exited green
after everything else had passed. The boundary now covers it, and `unexpected` is a plain
boolean rather than the exception object, so nothing about the object is ever read.

*Four appearances of one class, each a layer further out.* The lesson is that "the boundary
covers the work" was never checked against "which statements are actually inside it".

## Stage-2 repo review — round 26 (RUN_DIR `/tmp/cdx-review.9jwMqk`, `STATUS: completed`)

`SCOPE: branch diff against 96ead47 head=b5840097c123704f54630cb44dbeb5646d3f7ed0 dirty=false`

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| CDX26-01 | "[P1] Make the last-resort handler non-throwing — `wave_gate.py:2967-2970`. If the diagnostic sink itself raises… this call executes inside the `except BaseException` suite and is not caught by the same `try`. The exception therefore escapes `main()` and the process exits green" | **DC-2** (exception escaping a boundary, process exit status) — **sixth instance** | **CRITICAL** (blocking class *runtime behavior*: a green gate over an invalid run) | **fixed** |

The sixth instance of DC-2, and it was in the handler I had just written to end the class.
Reporting runs INSIDE an `except` suite, where a raise escapes the enclosing `try` — so a
throwing `_emit` exits green from the very handler that exists to prevent it.

**Sibling sweep (mandatory at second instance, re-run here):** all four `_emit` calls
inside `except` suites enumerated by AST walk. Two were genuine holes — the new
`GATE_UNEXPECTED_ERROR` emit and, older and previously unnoticed, the
`GATE_DIAGNOSTIC_UNRENDERABLE` FALLBACK emit, which had exactly the same shape. All four
now route through `_report`, a sink that cannot throw.

**Non-vacuity witness + coverage:** `test_a_throwing_diagnostic_sink_cannot_decide_the_exit_status`
drives a sink raising `SystemExit(0)` on BOTH paths. Measured:

```
A) unexpected error + throwing sink : main() -> 1
B) ordinary GateFailure + throwing sink : main() -> 2
```

Rendering (which can run foreign `__str__`/`__format__`) remains separately guarded, and
the exit status is decided before either rendering or reporting.

## Stage-2 repo review — round 27 (RUN_DIR `/tmp/cdx-review.WyFV2J`, `STATUS: completed`)

`SCOPE: branch diff against b584009 head=bfd75daf55977e8e01da6f173c82b5146d72d794 dirty=false`

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| CDX27-01 | "[P2] Retain the fallback when `_emit` rejects the diagnostic — `wave_gate.py:2919-2921`. When stderr remains usable but rejects the primary text — for example, it is configured as strict ASCII and `_refuse_ambiguous` produces its existing `—` diagnostic — this blanket swallow leaves stderr empty. The previous handler retried with the ASCII-only `GATE_DIAGNOSTIC_UNRENDERABLE`; after this refactor `main()` returns 2 without the required first-token code." | **DC-8** (a fix silently removing a property an earlier round established, the property's own test) — **third instance** | **standard** (machine-served contract: the first-token diagnostic code) | **fixed** |

Premise verified rather than assumed: `wave_gate.py:287` really does carry an em-dash in a
diagnostic message, so a strict-ASCII sink rejects it.

**This is a regression my own refactor introduced**, and the third of its kind in this
slice — after the render-request write losing `open_for_write`, and the inventory removal
losing the `follow_symlinks=False` walk. The pattern: closing one obligation
("reporting must not throw") by deleting the mechanism that met another ("a stable code
must still be printed").

`_report` is now a LADDER, each rung independently guarded: full text → caller-supplied
ASCII fallback (the bare code, `[A-Z_]+` by construction) → fixed ASCII line → give up
without raising. Measured:

```
A) em-dash text, ASCII fallback available : printed ['BASELINE_AMBIGUOUS']
B) em-dash text, no fallback              : printed GATE_DIAGNOSTIC_UNRENDERABLE...
C) every rung fails                       : returned normally (no raise)
```

**DC-8 second-instance action:** the class is "a fix removes a property an earlier round
established". The structural answer is that each such property must be pinned by a test
that fails when the property is removed — which is what the three witnesses now do
(`test_the_scratch_path_returned_is_the_path_that_was_verified`,
`test_cleanup_does_not_follow_a_replaced_owned_directory`, and
`test_reporting_still_prints_a_code_when_the_sink_rejects_the_message`). Sibling sweep:
every property established by a prior round in this slice now has a named regression;
the three that regressed did so because their properties were pinned only by the
narrower test of the round that introduced them.

## Stage-2 repo reviews — rounds 28 and 29

Round 28 (`/tmp/cdx-review.Op7F6Y`, base `bfd75da`) and round 29
(`/tmp/cdx-review.yqTnwY`, base `7f24fe4`). The manifest-transition claim recurred as the
FIFTH and SIXTH occurrence; both `finding-refuted` on the grounds already ratified by the
attested §6 gate.

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| CDX28-01 | "[P1] Append the new test instead of repointing manifest IDs" | — | P1 | **finding-refuted** (5th) |
| CDX28-02 | "[P2] Restrict fallback output to documented diagnostic codes — a provider-created or post-construction-mutated `GateFailure` can supply any matching token such as `UNDECLARED_CODE`; this shape-only check accepts it" | **DC-9** (a shape test standing in for membership in a closed set, the gate's own code roster) | **standard** (machine-served contract) | **fixed** |
| CDX29-01 | "[P1] Append the new manifest row without reassigning IDs" | — | P1 | **finding-refuted** (6th) |
| CDX29-02 | "[P1] Guard whitelist membership against hostile fallback objects — when `failure.code.__str__()` returns a `str` subclass with hostile `__hash__` or `__eq__`, this membership operation executes foreign code before `_report` reaches its guarded emission loop. It can raise `SystemExit(0)`, escape the `GateFailure` handler, and make a failed gate exit green" | **DC-2** (exception escaping a boundary, process exit status) — **seventh instance** | **CRITICAL** (runtime behavior: a failed gate exiting green) | **fixed** |
| CDX29-03 | "[P2] Validate the primary diagnostic before emitting it — this check clears only `fallback`, while `_report` still tries `text` first… `DIAGNOSTIC_CODES` is not actually authoritative" | **DC-9**, second instance | **standard** (machine-served contract) | **fixed** |

**CDX29-02 is the seventh DC-2 instance, and I introduced it while hardening the
reporter.** `x in DIAGNOSTIC_CODES` runs `__hash__`/`__eq__`, so the lookup meant to
SANITIZE a failure's code was itself a route onto the exit path. `exit_status_for`'s own
comment already documents four rounds of this exact class (`__name__` via a metaclass,
`__repr__` in a formatter, `__eq__` on an int subclass, `__hash__`/`__str__` on a str
subclass) — I reintroduced it through a containment test. Reproduced:
`ESCAPED as SystemExit -> would exit 0`.

`_own_code()` is now the single accessor: `type(raw) is not str` rejects subclasses before
any dunder can run (the exact builtin's `__hash__`/`__eq__` cannot be overridden), the
attribute read is guarded, and it is total. Measured after — hostile subclass survives,
`rc: 1`, `GATE_DIAGNOSTIC_UNRENDERABLE` printed.

**CDX29-03 — `DIAGNOSTIC_CODES` was not authoritative for the path that matters.** I
validated the fallback and left the primary text carrying `failure.code` directly:
`GateFailure("UNDECLARED_CODE", "m", 1)` printed `UNDECLARED_CODE m`. The primary text is
now built FROM the resolved code. A documented code still renders in full
(`WORKTREE_DIRTY details here`), so the check did not degrade every diagnostic to the
generic line.

**Harness note.** My first version of the DC-2 witness asserted the fallback rung while
passing a printable primary text — the ladder stopped at rung one and never touched the
hostile object, so the test would have proven nothing while passing. Corrected to
`text=None`. Fourth vacuity incident in this slice; all four found by checking what the
test EXECUTED rather than that it was green.

## Stage-2 repo reviews — rounds 30 and 31

- **Round 30** (`/tmp/cdx-review.i8vBb3`, base `05950e5`, head `706ec9d`): **CLEAN**, no
  findings. "The exact-string checks prevent hostile subclasses from reaching whitelist
  hashing, and diagnostics are now rendered only from documented codes."
- **Round 31** (`/tmp/cdx-review.N2cl0Y`, base `706ec9d`, head `8c61d59`): one finding,
  the manifest-transition claim for the **SEVENTH** time → `finding-refuted` on the
  grounds ratified by the attested §6 gate. Counts as clean (CLAUDE.md: "a round whose
  blocking findings were all removed by evidenced refutation counts as clean").

**On the recurrence itself.** Each Stage-2 round is scoped `--base <previous
last-reviewed-sha>`, so the reviewer always sees an intermediate branch commit as its
baseline and always reaches the same wrong conclusion about a manifest that is a
BOOTSTRAP at the only baseline that matters. The claim is structurally guaranteed to
recur under the delta-scoped review contract, and it is disposed of identically each
time: `dev` carries neither manifest, so `validate_transition()` never runs for the
landing push, and both required gates pass on every tip. Recorded once here rather than
re-argued per round.

## §6 architect review — round 11 (RUN_DIR `/tmp/cdx-gate-review.Hj8pIv`) — **ATTESTED**

`{"ok":true,"gate":"review","status":"completed","turnToken":1,"parsedVerdict":"ISSUES FOUND"}`

All five round-10 findings confirmed genuinely fixed. Four new findings.

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| ARCH11-01 | "[P1] Fingerprint mutation coverage can still pass after checking only one mutation… accepts `str` subclasses. Reproduced with one subclass whose `__eq__` matches every string: the roster check believed `semantic`, `envelope`, `policy`, and `revision` were all present, then `run_fingerprint_phase(True, provider)` returned `checked:1 case(s)` after exercising only one mutation." | **DC-9**, third instance | **CRITICAL** (runtime behavior: the #153 seam passing on one mutation while claiming four) | **fixed** |
| ARCH11-02 | "[P1] The required merge gate is explicitly still unconfigured… a PR can merge with the check failed, cancelled, or absent, contrary to the architect plan's Definition of Done." | — | **standard** (capability reachability) | **not-validated in-slice — recorded as OWED AFTER LANDING** |
| ARCH11-03 | "[P1] The deferral record is not closure-valid… #165 says that justification was falsified and re-decided as the single permitted `window-exhausted` deferral; its cited §6 round-9 checkpoint is absent, and both deferral records were updated before the ledger first entered the tree" | **DC-10** (a record citing an artifact that does not yet exist, the artifact) | **standard** (audit record validity) | **half fixed, half finding-refuted** |
| ARCH11-04 | "[P2] The ledger is stale after the closing commits… records six DC-2 instances although the hostile `str.__hash__` escape is explicitly the seventh; the checkpoint table also ends at evaluation 17" | **DC-7** | **standard** (served contract text) | **fixed** |

**ARCH11-01 — sibling sweep, with a measured split.** Ten `isinstance(..., str)` sites.
Nine validate JSON-derived values and `json.loads` yields exactly `str` (measured), so a
subclass cannot reach them; the tenth, `_provider_strings`, takes PROVIDER data — the
trust boundary the #153 seam exists to police — and is now `type(item) is not str`.
Reproduced closed: `refused: PLAN_FINGERPRINT_MISMATCH`, plain strings unaffected.

**ARCH11-02 — accurate, and NOT satisfiable in this slice.** Measured:
`gh api repos/.../rulesets` returns `[]` — no ruleset exists, so `README.md:594` is
CORRECT rather than stale. GitHub will not accept a required status check that has never
run, so the order is land → the workflow runs → configure the ruleset. Recorded in the
ledger under "Owed after landing" and carried to the final report; NOT counted as done.

**ARCH11-03 — the half that was right is the one that mattered.** The claim that #165's
body was "re-decided as `window-exhausted`" is false: its body reads
`out-of-scope-by-design` … `Not window-exhausted` — `finding-refuted` with the issue body
as evidence. But the ordering complaint is correct and was mine: both deferral records
were written BEFORE the ledger entered the tree, so at the moment of recording their
cited row did not exist. Remedied by committing a closing checkpoint (`711d0e8`) and only
THEN re-citing both issues against it — the ledger's §6 checkpoint rows were added in the
same pass.

## §6 architect review — round 12 (RUN_DIR `/tmp/cdx-gate-review.7tLUAe`) — **ATTESTED**

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| ARCH12-01 | "[P1] `wave_gate.py:1507` trusts the last summary-shaped line. Reproduced: a real `9740 passed, 33 skipped` summary followed by an atexit line claiming `9773 passed` is parsed as 9,773 passed and zero skipped, so `run_suite()` returns success." | **DC-11** (a check applied to a DERIVED view rather than the authority) | **CRITICAL** (runtime behavior: the suite arm reporting success for a failing/mass-skipped run) | **fixed** |
| ARCH12-02 | "[P1] `ISSUE_152_AUDIT_LEDGER.md:140` incorrectly refutes the #165 lineage finding. The live issue body contains both the new out-of-scope-by-design declaration and the later explicit 're-deferred once as window-exhausted' disposition." | **DC-12** (asserting absence from a PARTIAL read) | **standard** (audit record validity) | **finding CONFIRMED — my refutation was wrong** |
| ARCH12-03 | "[P1] `ENDGAME_VERIFICATION_GATE.md:5` confirms rollout remains unevidenced… the slice cannot close with ruleset configuration as its only remaining step." | — | **standard** (capability reachability) | **confirmed — owed list corrected** |

**ARCH12-02 — I was wrong, and the way I was wrong matters.** I checked #165's body with
`head -12`, saw `out-of-scope-by-design`, and recorded the reviewer's claim as
`finding-refuted`. Line 68 of that same body carries a
"Re-decision 2026-08-14 (falsified-justification rule)" section disposing it exactly as
claimed. **Absence asserted from a partial read** — the precise failure this slice has
flagged in other contexts, committed inside the audit record itself. The ledger now
records the correction, and #165's header has been superseded so header and re-decision
agree on `window-exhausted`.

Substantively: #165's original ground (a ~4.9 s registry import) was measured in-slice at
~0.39 s, and the amendment voids a justification contradicted by an in-slice measurement.
`window-exhausted` is the honest class, and it consumes the finding's single permitted
one.

## Stage-2 repo review — round 32 (RUN_DIR `/tmp/cdx-review.1YSFH7`)

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| CDX32-01 | "[P1] Track summaries before truncating the stream — `wave_gate.py:1526`. When an `atexit` handler or plugin emits 400 or more lines after pytest's genuine summary and ends with a fabricated passing summary, `run_suite` has already evicted the genuine line from its 400-line buffer. This check then sees exactly one summary, reports zero skips, and can let a mass-skipped suite pass" | **DC-11**, second instance | **CRITICAL** (runtime behavior) | **fixed** |

**DC-11 is now structural.** Both instances are the same mechanism: a guard enforced
against a DERIVED artifact instead of the authority it summarizes. First the parser took
the LAST matching line rather than requiring one; then the one-summary rule was applied to
a bounded 400-line ring kept for error context. Summary candidates are now counted WHILE
STREAMING, and `tail` is only ever error context.

Proven against a reconstructed pre-fix copy — the regression genuinely bites:

```
PRE-FIX run_suite returned: {'passed': 9773, 'failed': 0, 'skipped': 0, 'errors': 0}  <-- the forgery wins
NEW parse refused: PYTEST_SUMMARY_AMBIGUOUS
```

**Test-design note:** the regression drives `run_suite` itself, not `_parse_suite_summary`.
A parser-level test would have passed against the broken version, because the parser was
never the defect — the defect was WHERE the rule was applied. Sibling sweep: `tail` is the
only bounded buffer in the gate from which a decision was drawn; every other check reads
its authority directly.

## Stage-2 repo review — round 33 (RUN_DIR `/tmp/cdx-review.nWoakt`)

`SCOPE: branch diff against d90a315 head=139c862d701c11a51f21269e4ecf51982b98a719 dirty=false`

One finding — the manifest-transition claim for the **EIGHTH** time → `finding-refuted`.
No other findings, so the delta is clean and the round counts as clean.

## §6 architect review — round 13 (RUN_DIR `/tmp/cdx-gate-review.4i4Aaw`) — **ATTESTED**

Confirmed my three round-12 fixes and the ledger/#165 corrections. Three new findings.

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| ARCH13-01 | "[P1] Count outcome summaries, not newline records: a local hook can append forged and genuine outcomes to one physical line — `100 passed, 0 skipped in 0.01s 100 skipped in 0.01s` — the parser treats it as one candidate and reads the first counts, so `run_suite` returned green with all 100 tests skipped" | **DC-11**, third instance | **CRITICAL** (runtime behavior) | **fixed** |
| ARCH13-02 | "[P1] Reconcile every execution outcome with collection: a hook can retain a required node during collection but deselect it only during execution; reproduced `229 passed, 1 deselected`, exit 0, with 230 collected, and the gate returned success because `deselected` is ignored and the missing pass fits inside skip-cap headroom, so a required golden test can avoid execution" | **DC-13** (an outcome vocabulary open by omission, pytest's actual outcome set) | **CRITICAL** (runtime behavior — this defeats the gate's central claim) | **fixed** |
| ARCH13-03 | "[P2] Validate the complete render envelope: `_render_pass` ignores `len` and `sha256`, accepts unknown fields, and uses permissive base64 decoding" | **DC-13**, second instance | **standard** (runtime behavior) | **OPEN — next batch** |

**ARCH13-01 — counting lines is not counting outcomes.** Verified:
`{'passed': 100, 'failed': 0, 'skipped': 0, 'errors': 0}` for a fully skipped run. The
grammar is now anchored (`_SUMMARY_LINE_RE`) with exactly one duration clause, and a
repeated clause is `PYTEST_SUMMARY_AMBIGUOUS` rather than first-wins. The real line still
parses — a stricter grammar that rejected honest output would be worse than the hole.

**ARCH13-02 — the vocabulary was open by omission, and that defeats the whole gate.**
Verified: `'deselected' mentioned in the gate: False`, and
`229 passed, 1 deselected` → `{'passed': 229, 'skipped': 0}`. A required node retained at
COLLECTION and deselected at EXECUTION vanished into skip-cap headroom. Two changes:
the outcome set is now CLOSED (`PYTEST_OUTCOME_UNACCOUNTED` for anything the gate cannot
account for, while `failed`/`error` stay parseable so the specific `PYTEST_FAILED`
diagnostic still wins), and an **accounting identity** requires
`passed + skipped == len(collected)`. Confirmed against a real run:
**9760 + 18 = 9778 = collected**.

### Three process failures in this round, recorded because they nearly shipped

1. **The same ambiguous-`sed`-anchor mistake, third occurrence** — a code inserted into a
   RAISE SITE instead of the frozenset, twice in one round. Now using line-precise edits
   for these.
2. **An edit that silently did nothing.** The accounting-identity insert used an anchor
   (`return summary` followed by `def`) that does not exist — a comment block sits
   between them — so `.replace()` matched nothing and reported success. The regression I
   had written failed with `DID NOT RAISE`, which is the only reason it was caught. Had I
   trusted the edit and skipped the test, the gate would have shipped without the check
   while the ledger claimed it was added.
3. The regression drives `run_suite`, not the parser, because the identity lives where
   collection and outcome meet.

**ARCH13-03 remains OPEN** and is the next correction batch: `_render_pass` must enforce
the exact child envelope — declared `len`, `sha256` digest, canonical base64, no unknown
fields — rather than trusting `b64` alone.

## Stage-2 repo review — round 34 (RUN_DIR `/tmp/cdx-review.NjWpSN`)

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| CDX34-01 | "[P2] Normalize error aliases before duplicate detection — `wave_gate.py:1604`. When a zero-exit pytest run emits both accepted spellings — for example `1 passed, 1 error, 0 errors in 1.0s` — the duplicate check treats them as separate outcomes and this plural-first lookup records zero errors… `run_suite()` then satisfies the accounting identity and returns success despite the reported error" | **DC-14** (a guard keyed on a SURFACE FORM rather than the canonical value) | **standard** (runtime behavior) | **fixed** |

Reproduced: `{'passed': 1, 'failed': 0, 'skipped': 0, 'errors': 0}` — the reported error
vanished. Spellings are now canonicalized (`_OUTCOME_ALIASES`) BEFORE duplicate detection,
and nothing downstream sees a raw spelling. Verified after: the collision is
`PYTEST_SUMMARY_AMBIGUOUS`; `1 error` → `errors: 1`; `2 errors` → `errors: 2`; the real
line unchanged.

**DC-14 names a pattern that ran through three consecutive rounds.** Each time I added a
real guard whose KEY was more permissive than the property it protected:

1. round 12 — the guard counted LINES, not outcomes;
2. round 13 — the outcome vocabulary was open by omission (`deselected` unaccounted);
3. round 34 — two spellings of one outcome were distinct keys.

The structural answer, now applied at the boundary: parse into a CANONICAL form first, and
let every later check read only canonical values. Sibling sweep of the same parser: the
grammar is anchored, the key set is closed, aliases are mapped, and the accounting
identity ties the result back to collection — there is no remaining surface form a check
keys on.

## Stage-2 repo review — round 35 (RUN_DIR `/tmp/cdx-review.TR8Pdz`)

`SCOPE: branch diff against 917ea65 head=de4d0ed9ff870c8ca0580bb1fd18e67e38f1c4ab dirty=false`

One finding — the manifest-transition claim for the **NINTH** time → `finding-refuted`.
No other findings; the round is clean.

## §6 architect review — round 14 (RUN_DIR `/tmp/cdx-gate-review.kD3sjA`) — **ATTESTED**

Confirmed all three round-13 reproductions closed. Three new findings, all fixed.

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| ARCH14-01 | "[P1] Execution is reconciled only by aggregate count, not node identity. Reproduced with collected nodes A and B, where B always fails, plus a `pytest_collection_modifyitems` hook that replaces B with A only during the full run… `run_suite()` accepts because `passed + skipped == len(collected)`" | **DC-15** (an aggregate standing in for identity) | **CRITICAL** (runtime behavior — the gate's central claim is that every required node RAN) | **fixed** |
| ARCH14-02 | "[P2] GitHub event JSON accepts duplicate authority fields with last-value-wins semantics… the manifest phase selected the later pair and exited 0 instead of rejecting the ambiguous event" | **DC-16** (a document ambiguous about itself, silently resolved) | **standard** (runtime behavior) | **fixed** |
| ARCH14-03 | "[P2] Render-envelope JSON has the same duplicate-member hole" | **DC-16**, second instance | **standard** (runtime behavior) | **fixed** |

**ARCH14-01 — my own accounting identity was necessary but not sufficient.** A hook that
replaces node B with a second copy of node A keeps `passed + skipped == len(collected)`
exactly true while B never executes. The gate now writes a junit execution record into its
OWNED scratch and compares the multiset of `(classname, name)` records against the same
projection of the collected node ids — catching a missing node and a doubled one alike.

**ARCH14-02/03 — one root, fixed once.** `json.loads` keeps the LAST value for a duplicate
key. `_strict_json_loads` (built on the pre-existing `_no_duplicate_keys`, which the
manifest parser had used all along) now rejects duplicate members at every site where the
gate reads outside JSON.

### The most instructive failure of the slice

The new reconciliation **failed on its first honest run**, and it was right to:

```
PYTEST_EXECUTION_UNRECONCILED ... Missing: [('tests.test_loopback_redirect_patch.test_loopback_port_flexibility[http://[', '1]/callback-True]')]
```

The projection split node ids on `::` — but a PARAMETER can contain `::`, and this suite
has `test_loopback_port_flexibility[http://[::1]:9999/callback-...]`, an IPv6 literal. My
three hand-written unit-test node ids were all tidy and all passed; the 9,782-test run
found it immediately. The parameter section is now split off before the separators, and
that real node id is in the regression.

Two lessons, recorded because they generalise:

* **A stricter check is a liability until it has been run against reality.** Had the green
  unit tests been treated as sufficient, the gate would have failed on EVERY CI run —
  converting a fail-open into a fail-closed that blocks the branch. The strictness was
  right; the parser was not.
* **I chose example inputs that exercised the shapes I was thinking about** (module, class,
  parameter) rather than the shapes the repository actually contains. The suite's own node
  ids were the better oracle and were available the whole time.

## Stage-2 repo review — round 36 (RUN_DIR `/tmp/cdx-review.uFbrV2`)

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| CDX36-01 | "[P1] Preserve existing IDs when extending the node manifest" | — | P1 | **finding-refuted** (10th) |
| CDX36-02 | "[P1] Secure the JUnit artifact before handing it to pytest — `own()` merely claims the name without exclusive creation or an inode proof. Pytest opens this pathname with `w` at session finish, so a planted symlink can truncate an arbitrary file, while replacing the report before `_executed_projections()` can forge the execution evidence" | **DC-4** (ownership asserted rather than established) — **third instance, reintroduced via a CHILD's write** | **CRITICAL** (data loss + forged evidence) | **fixed** |
| CDX36-03 | "[P2] Reject collisions in projected JUnit identities — `tests/a.py::B::test_x` and `tests/a/B.py::test_x` both project to `('tests.a.B', 'test_x')`… omitting one and running the other twice leaves these projected multisets equal" | **DC-15** (an aggregate standing in for identity), second instance | **standard** (runtime behavior) | **fixed** |

**CDX36-02 — the class I had already closed, reintroduced at a new kind of site.** Rounds
12–24 established that the gate owns what it writes; this was a write DELEGATED to a child
process, and I reverted to claiming a name. `open_for_write` now creates the report
exclusively (`O_EXCL|O_NOFOLLOW`), a descriptor is held, `_refuse_unowned_report` proves
the named file is still the created inode, and `_executed_projections` parses through the
descriptor rather than the path.

**CDX36-03 — a hole I had already seen and dismissed.** While designing the projection I
noticed the collision and judged it exotic. It defeats the entire claim: multiset equality
over a non-injective projection is not identity. Measured before fixing: the reviewer's
example DOES collide, and there are **0 collisions among the 9,784 active node ids** — so
the injectivity assertion constrains nothing real and fails closed the day a colliding
pair is added.

*Recorded because it is the sharper lesson of the two:* I had the finding and reasoned it
away as unlikely. "Unlikely" is not a property a gate can rely on, the check is three
lines, and the measurement showed it costs nothing.

## Stage-2 repo review — round 37 (RUN_DIR `/tmp/cdx-review.HRHwN6`)

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| CDX37-01 | "[P1] Preserve existing manifest rows and append the new tests" | — | P1 | **finding-refuted** (11th) |
| CDX37-02 | "[P1] Retain the exclusive-create identity when opening the report — `close()` discards the only descriptor tied to the `O_EXCL` creation before `report_fd` is opened. A sibling can atomically replace `junit.xml` with a hard link to another same-filesystem file in that gap; the descriptor and later pathname stat then identify the same foreign inode, so the ownership check passes and pytest truncates the foreign file with `w`." | **DC-4**, fourth instance | **CRITICAL** (data loss) | **fixed** |

Verified in both directions:

```
OLD shape (create, close, reopen by name): ownership check PASSED on the foreign inode (bad)
NEW shape (create and KEEP the descriptor): refused PYTEST_EXECUTION_UNRECONCILED
                                            victim intact: True
```

`create_owned()` returns the raw descriptor from the `O_EXCL` creation and it is never
closed and reopened, so the anchor cannot be swapped underneath the check.

**Residual NOT claimed as closed → #164, third site.** pytest opens the report BY NAME
with `w`, and no portable mechanism makes a child write through an inherited descriptor. A
same-user process replacing the name between creation and pytest's open still causes pytest
to truncate whatever the name points at. The gate refuses afterwards and never accepts the
result, but the write has happened. #164's acceptance criterion now requires any mechanism
to cover all THREE sites: the scratch root, owned child directories, and the child-written
execution report.

## Convergence assessment at rounds 30–37

| Round | Defect found in | Class |
| --- | --- | --- |
| 30 | — | clean |
| 32 | my one-summary rule | guard on a DERIVED view |
| 33 | — | clean (refuted only) |
| 34 | my grammar fix | guard keyed on a SURFACE FORM |
| 35 | — | clean (refuted only) |
| 36 | my identity check | ownership by name; non-injective projection |
| 37 | my ownership fix | anchor discarded by close/reopen |

Every one of these was a real defect, and every one was in code written to fix the
previous round — which is the signature of hardening a subsystem against a same-user
adversary, exactly the class already deferred to **#164**.

**Operative stop criterion, stated before the next round rather than after it:** continue
fixing anything that lets the gate PASS WHEN IT SHOULD FAIL under ordinary operation;
route further *same-user-racing-the-gate* variants to #164 rather than chase them one
syscall at a time. Rounds 36 and 37 were the latter class and were fixed anyway because
each had a cheap, exact remedy; a variant without one goes to #164.

## Stage-2 repo review — round 38 (RUN_DIR `/tmp/cdx-review.dBUgoz`) — **CLEAN**

No findings at all. "The patch correctly retains the descriptor returned by exclusive
creation, eliminating the close-and-reopen substitution window."

## §6 architect review — round 15 (RUN_DIR `/tmp/cdx-gate-review.ad8BR0`) — **ATTESTED**

The reviewer independently verified, rather than accepting my report: "all 9,785 active
node IDs project exactly as pytest 9.0.2 does, with zero pre- or post-XML-escaping
collisions, and all 240 `test_wave_gate.py` tests pass." It confirmed the three round-14
findings closed and found ONE new contract gap.

| Source ID | Verbatim summary (abridged) | Defect class | Tier | Disposition |
| --- | --- | --- | --- | --- |
| ARCH15-01 | "[P2] `_strict_json_loads` still accepts non-standard `NaN` and `±Infinity`. A push event with valid `before`/`after` plus `\"commits\": NaN` is accepted and can complete green, contrary to the requirement that malformed events fail with `BASELINE_EVENT_INVALID`." | **DC-16** (a document ambiguous/invalid about itself, silently accepted), third instance | **standard** (machine-served contract) | **fixed** |

Reproduced: `ACCEPTED non-standard JSON: {'commits': nan}`. Python's decoder accepts
`NaN`/`Infinity`/`-Infinity`; JSON does not. `parse_constant` now rejects all three, and
ordinary JSON is unaffected. Verified end to end through the real CLI:

```
BASELINE_EVENT_INVALID cannot read event payload /tmp/nan-event.json: non-standard JSON constant 'NaN'
```

**A finding the reviewer explicitly declined to raise, recorded so it is not lost:** a
deceptive in-process `conftest.py` can rewrite a callable and its private identity
metadata, but the gate's documented trust boundary trusts executed test code, so it is not
an ordinary-operation bypass. That is the same boundary this slice established at round 7
and it is consistent with #164's scope.

## Stage-2 repo review — round 39 (RUN_DIR `/tmp/cdx-review.UBGkaP`)

One finding, the manifest-transition claim for the **TWELFTH** time → `finding-refuted`.
The same review states "The JSON decoder hardening is sound." Clean.

## §6 architect review — round 16 (RUN_DIR `/tmp/cdx-gate-review.bFYEn9`) — **ATTESTED, NO ISSUES**

`{"ok":true,"gate":"review","status":"completed","turnToken":1,"parsedVerdict":"NO ISSUES"}`

The reviewer verified independently rather than accepting the summary: "all three
non-standard constants are rejected through the real CLI with exit 2 and
`BASELINE_EVENT_INVALID`", "the gate tests pass (241 passed)", "canonical collection
matches all 9,786 manifest nodes", "both protected trees (`src/` and golden bytes) remain
unchanged from `9080e3c`", and "the worktree is still clean". Its conclusion:

> "No ordinary-operation fail-open path was reproduced. The code is ready to land; #152
> should remain open for the documented post-landing Actions, seeded-defect, revert, and
> ruleset evidence."

# CLOSING CHECKPOINT — code complete, slice NOT closed

- **Stop criterion, declared in advance:** two consecutive clean gate results on the final
  tree — one Stage-2 repo review and one §6 architect review — with every required gate
  current, zero unresolved criticals, and an accurate ledger and deferrals.
- **Met at `b2b608b`:** Stage-2 round 38 CLEAN (no findings), round 39 clean
  (refutation-only), §6 round 16 **attested NO ISSUES**.
- **Per-tier residue:** critical **0** unresolved. Standard: 0 unresolved in-slice; two
  deferred to filed, slotted issues (#164 `blocked-by-mechanism`, three sites; #165
  `window-exhausted`, consuming its single permitted one).
- **Validation current on this tree:** wave gate exit 0 — manifests ok (9786), collection
  ok, 9768 passed / 18 skipped, per-node identity reconciliation OK, 60 goldens
  deterministic and byte-exact, `PLAN_FINGERPRINT_PENDING issue=#153`.
- **Darkness holds throughout:** `src`, `examples` and `tests/fixtures/golden_xml`
  unchanged from `9080e3c` at every commit.

**OUTCOME: `DEFER-STANDARD-AND-PROCEED` to the landing step.** NOT `CLOSE-CLEAN`: the
plan's §6 rollout evidence is unfinished and cannot be produced before the workflow exists
on `dev`. #152 stays OPEN until the real Actions run, the five seeded-defect scratch-branch
proofs with red URLs recorded in the issue, the reverts proven green, and the `dev` ruleset
are all done.

# ROLLOUT EVIDENCE (post-landing)

## 1. Landing run — PASS

`dev` fast-forwarded `9080e3c..b2b608b`. Run
<https://github.com/RenEra-ai/boomi-mcp-server/actions/runs/31870522938> → **success**.

```
interpreter ok: 3.11.15 (main, Jun 16 2026) [GCC 13.3.0]
wave_gate: baseline 9080e3c… (push)
wave_gate: BOOTSTRAP — manifests are introduced by this change
wave_gate: manifests ok (9786 required nodes, 60 active goldens)
wave_gate: collection ok (9786 tests)
9767 passed, 19 skipped, 20 warnings in 2209.93s (0:36:49)
PLAN_FINGERPRINT_PENDING issue=#153
```

Two results worth more than the green tick:

* **CI took the BOOTSTRAP path**, settling on real infrastructure the claim refuted twelve
  times against intermediate baselines.
* **19 skipped on the runner vs 18 locally**, and `9767 + 19 = 9786 = collected`. The
  accounting identity holds with a DIFFERENT skip count, so it is not tuned to this
  machine; the extra skip is the `gcloud`-conditional the cap of 30 was sized for.

Noted risk: 36:49 against a 60-minute job timeout (61% consumed) versus 12:55 locally.

## 2. Seeded-defect proofs

Mechanism: the workflow triggers on `push:[dev]` and `pull_request:[dev]`, so a scratch
branch alone triggers nothing — each seed is a short-lived PR to `dev`, which also
exercises the **`pull_request` baseline arm** (merge-base) that had never run before.

| Seed | Defect | Expected | Result |
| --- | --- | --- | --- |
| 2 (#167) | module fails on import | `PYTEST_COLLECTION_FAILED` | **CONFIRMED**, run 31872214622 |
| 4 (#169) | `minimum_collected` → 99999 | `PYTEST_COLLECTION_FLOOR` | **CONFIRMED**, run 31872217332 |
| 3 (#168) | required test removed | `PYTEST_NODE_MISSING` | first attempt got `PYTEST_COLLECTION_FLOOR` — **my seeding error**, re-seeded |
| 5 (#170) | golden + row deleted | `MANIFEST_TRANSITION_ILLEGAL` | first attempt got `MANIFEST_FORMAT_INVALID` — **my seeding error**, re-seeded |
| 1 (#166) | one golden byte mutated | `GOLDEN_MISMATCH` | pending (needs the full ~37 min suite first) |

**Both mismatches were mine, and the distinction matters.** Seed 3 dropped collection to
9785 against a floor of 9786, so the floor check fired first — the plan says "leaving
enough other tests to exceed the floor" and my seed did not. Seed 5 deleted a MIDDLE row,
breaking sequential id numbering, which parsing catches before transition validation. In
both cases the gate refused the defect and the earlier check was the correct one; what was
not yet proven is the specific path the plan names.

Re-seeded: seed 3 now removes a required test AND adds a replacement so collection stays at
the floor; seed 5 now deletes the LAST row so id sequencing stays valid and the transition
check is what fires.

*Recorded rather than smoothed over:* "the run went red" is weaker evidence than "the run
went red for the predicted reason". Accepting the first attempt would have produced two
green-looking evidence rows for paths that were never exercised.
