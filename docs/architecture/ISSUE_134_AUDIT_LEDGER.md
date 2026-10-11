# Audit ledger — issue #134 slice: development-only revision-freshness deferral

Instantiated at Stage-1 step 0 from `templates/AUDIT_LEDGER_TEMPLATE.md`, committed in the
Stage-1.5 baseline commit before the first Stage-2 round. #134 is the M12 epic; this ledger
covers ONE tooling slice tracked under it (owner's choice: "select from existing, parallel-safe"
— no open issue is about tooling, and every other M12 slice is sequenced behind #186). Closing
this slice does not close #134.

Conventions inherited from #152's end state: no hand-maintained totals or instance lists —
counts are derived from the rows; every derived field carries its deriving anchor inline;
platform-behaviour claims carry a provenance marker (`measured here` / `documented, not
measured` / `assumption`); measured output is quoted, never re-keyed from memory;
audit-record-integrity findings are non-blocking from round one; diagnostic codes named
anywhere in this file exist in the gate's `DIAGNOSTIC_CODES` and sit inside a backtick span or
fence body. The switch's environment-variable name is written WITHOUT backticks here: it is not
a diagnostic code, and the scanner reads every backticked upper-snake token as one.

## Baseline (Stage-1 step 0)

- Issue: #134 (M12 epic) — slice "development-only revision-freshness deferral"
  (BOOMI_DEFER_REVISION_GATES=1), design reviewed with Codex 2026-10-08 (two read-only rounds,
  verdict "adopt with changes")
- Step-0 baseline: `46100b7866c7962b53cf62a400276ae30258ccc6` (`origin/dev`, contains #184)
- Branch: `codex/defer-revision-gates`, worktree; lands AFTER #186 (rebase onto it, redo the
  `test_nodes.jsonl` rows — the only file both branches touch, measured by
  `git diff --stat origin/dev...codex/issue-186`)
- Slice kind: **DARK to the Boomi MCP runtime.** No change under `src/`, `server.py`,
  `server_http.py` or the golden XML corpus (`git diff 46100b78 -- src server.py server_http.py`
  is empty, measured here). Behaviour-affecting for the test plugin, the gate CLI and the two
  inventory producers. Stage-1 QA is therefore a darkness proof plus end-to-end exercise of the
  changed CLI/plugin surface, not a live Boomi matrix.
- Artifact trust boundary:
  - **Creates and owns** — the switch and its parsing, the corpus plugin's deferral branch and
    summary, the `revision_freshness` marker and its two-way bind (`committed_artifact`), the
    gate's `REVISION_DEFERRAL_PRESENT` refusal, the producers' refusals, the rule text, this
    ledger and its archive.
  - **Consumes from outside** — pytest's hook order (terminal summary after
    `pytest_sessionfinish`; collection-modifyitems before runtest), argparse, the process
    environment, git. Hook-order claims are `measured here` (the deferred runs below print
    the summary after the session's last report).
- Expected defect classes, pre-enumerated so a second instance triggers structurally ON ARRIVAL:
  - **environment inheritance into a child that must be strict** — every child builder copies
    the parent environment; the authority is the single refusal/drop point per entry.
  - **hand-enumeration shadowing a derivable authority** — the generic class; here the
    freshness-test set, and the spellings of the variable across standalone scripts.
  - **a freshness comparison hiding inside a behaviour test** — the class this slice exists to
    split; the authority is the marker bind.

## Profiling (the owner's go/stop gate, run before any switch code)

Rule agreed with the owner: if the per-batch saving were under ~15 minutes, ship only the
single-build rebaseline and stop. Measured here, 2026-10-10, three jobs concurrent with one
unrelated #186 harvest on a 10-core host (load ≈ 2):

| Producer | Command | Wall time |
| --- | --- | --- |
| corpus harvest, `dev` corpus (372 KB) | `tests/_revision_corpus.py --write --corpus <tmp>` | `real 751.15` |
| inventory, one build | `tests/_m12_12_legacy_inventory.py --check` | `real 39.41` |
| rebaseline, two builds (before) | `scripts/rebaseline_m12_12_inventory.py --check` | `real 73.03` |
| rebaseline, one build (after) | same | `real 33.28` |

The #186 harvest on its 719 KB corpus had been running for 22:22 (`ps etime`) when measured
and had not finished. Decision: **GO** — a batch that adds covered inputs pays ≥ 12.5 min
(dev-sized) and > 22 min (slice-sized) of harvest. The contract snapshots have no producer
(hand-regenerated, seconds).

## Loop roster (fixed BEFORE the first correction; a gate not listed here cannot mint a loop mid-run)

1. **Stage-1 QA** — darkness proof (src/golden diff + stash differential) plus end-to-end
   exercise of the changed surface: strict and deferred suite runs, the gate CLI refusal, the
   producers' refusals.
2. **Stage-2 repo Codex review** — delta-scoped; every round COLLECTED via the collector,
   never read from `wait`.
3. **Composite wave gate** — full non-KB suite strict + every active golden + determinism +
   manifests (`scripts/wave_gate.py wave`), on the final tree, re-run after the rebase onto
   #186.
4. **Scratch preflight** — `Python 3.11 non-KB` green on `scratch/**` before the dev FF.
5. **Terminal correction loop** — ONLY via a recorded roster-addition checkpoint.

## Defect-class ledger

A class is a (mechanism, runtime-authority) pair, assigned at reconciliation, revisable with the
original retained.

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| DC-1 | environment copied wholesale into a child or a gate run that must not inherit what the plugin reads | the plugin's own env reads, funnelled through `_env` over the plugin's variable tuple | DSN-03, CDX-134-r1-01 | 2nd instance → structural: `_env` funnel + `child_env` drops all of the plugin's variable tuple; entry refusals unchanged |
| DC-2 | freshness comparison embedded in a behaviour test | the `revision_freshness` marker + `committed_artifact` bind | DSN-04 | split at every site in the sibling sweep below |
| DC-3 | set-level pin that cannot see one of two markers on one artifact go missing | per-test runtime bind | DSN-05 | `committed_artifact` refuses an undeclared read; an unread marker fails its test |

## Sibling sweep (DC-2), from the exploration inventory of every comparison against the four artifacts

- `test_the_whole_contract_is_frozen_in_a_committed_snapshot` — marked; its render-twice
  determinism half moved to `test_the_served_contract_renders_identically_after_a_cache_reset`
  (now with a cache reset between renders, which the old second assertion lacked).
- `test_the_page_schema_is_frozen_in_a_committed_snapshot` — marked (pure freshness).
- `test_rewriting_the_label_legend_moves_the_revision` — fixture branch removed: a strict
  subset of the whole-contract byte comparison.
- `test_the_two_sentences_this_slice_falsified_are_gone` — fixture branch KEPT: it checks
  fixed strings, which a stale snapshot cannot reintroduce, so it is not a freshness
  comparison.
- `test_the_served_state_mappings_match_the_committed_fixture_by_value` — renamed to
  `..._match_the_current_projection_by_value` and compares the served page with the current
  revision payload (behaviour); the fixture side is the whole-contract test's.
- `test_served_artifacts_match_the_committed_values` — marked; reads through the helper.
- `test_legacy_callers_and_process_kind_producers_match_the_baseline` — keeps every drift kind
  except a changed served-artifact value (owned by the marked test); artifacts added/removed
  stay here as reachability growth.
- `test_the_served_collection_cannot_touch_boomi_transport` — compares with the module's own
  derivation instead of the committed baseline.
- Out of scope, one line each: the corpus integrity tests (`test_every_packaged_input_replays_
  to_a_verdict`, `test_the_packaged_corpus_is_the_producer_s_own_serialization`) check
  integrity, not revision churn; `process_ir_v1.schema.json` / `system_topology_v1.schema.json`
  snapshots are not moved by a revision; the `_issue_157_*` and connector-replay producers write
  no revision artifact.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DSN-01 | Codex design consultation, round 1 (read-only, 2026-10-08; not a roster gate) | "Do not disable the corpus plugin or blanket-skip revision sweeps." | none (design review) | runtime behavior | n/a (design input) | Standard — anchor: no source label, no critical class | step-0 design | `fixed` — plugin stays installed; only the absent-input branch defers; sweeps unmarked and running (`test_a_deferred_run_records_an_absent_input_as_pending_and_keeps_every_other_refusal`, `test_a_deferred_run_still_refuses_a_funnel_reached_around_the_entry_points`) |
| DSN-02 | Codex design consultation, round 1 | "Accept only the exact value `1` in local development pytest. Any other present value is an error" | none (design review) | runtime behavior | n/a (design input) | Standard — anchor: no source label, no critical class | step-0 design | `fixed` — `deferral_requested`; `test_any_other_value_is_a_usage_error_rather_than_a_quiet_answer` |
| DSN-03 | Claude pre-consultation read of `scripts/wave_gate.py:1370`, confirmed by Codex round 1 | "`_pytest_env` copies `os.environ` wholesale into the pytest child. So a switch exported in a developer's shell WOULD leak into a local `wave_gate.py` run" | none (design review) | machine-served schemas/contracts | DC-1 (1st instance) | Standard — anchor: no source label, no critical class | step-0 design | `fixed` — `REVISION_DEFERRAL_PRESENT` at `main()`; `test_the_revision_deferral_switch_is_refused_whatever_its_value`, `test_every_gate_subcommand_refuses_the_revision_deferral_switch`, `test_no_gate_child_inherits_the_revision_deferral_switch` |
| DSN-04 | Codex design consultation, round 1 | "`test_the_served_state_mappings_match_the_committed_fixture_by_value` … Mixed: freshness **and public-wrapper correctness**" | none (design review) | runtime behavior | DC-2 (1st instance) | Standard — anchor: no source label, no critical class | step-0 design | `fixed` — split per the sibling sweep above |
| DSN-05 | Codex design consultation, round 2 | "Concrete hole: two freshness nodes compare the same artifact. Removing either marker leaves `{artifact paths}` unchanged" | none (design review) | runtime behavior | DC-3 (1st instance) | Standard — anchor: no source label, no critical class | step-0 design | `fixed` — per-test bind; `test_a_marker_and_its_comparison_imply_each_other` |
| DSN-06 | Codex design consultation, round 1 | "Rebaseline invokes `--write`, then `--emit-markdown`; both call `build_inventory()`." | none (design review) | runtime behavior | n/a (cost, not a hand-model) | Standard — anchor: no source label, no critical class | step-0 design | `fixed` — one emitter run; `--check` up to date, `real 73.03` → `real 33.28` |
| DSN-07 | Codex design consultation, round 1 | "“Callers only see dev” is not established by this repository." (Cloud Build deploys `main` running no test gate) | none (design review) | capability reachability | n/a (pre-existing deploy gap) | Standard — anchor: no source label, no critical class | step-0 design | `severity-refuted` — Codex round 2 agreed: `origin/main` carries no corpus (1387 commits behind `origin/dev`, measured here) and the deploy path never ran tests, so the gap predates this switch; the rule carries the publication sentence; enforcing it in Cloud Build is outside this slice |
| QA-134-r1-01 | Stage-1 QA r1 (`agents/reports/2026-10-10-issue-134-defer-revision-gates-r1.md`, gitignored) | "the slice changed `test_the_served_collection_cannot_touch_boomi_transport` to compare against `derived` instead of the committed baseline. Both sides are now collected under the same transport bomb … a served producer that reaches the Boomi transport and swallows the error" passes it; only the deferral-skipped freshness test still fails | Low | runtime behavior (a behavioural guard weakened into the deferral's skip set) | DC-2 (2nd instance: a behavioural check whose only remaining detection was a freshness comparison) | Standard — anchor: source label Low, no critical class | working tree on `46100b78` | `fixed` — the sentinel RECORDS each intercepted verb before raising; the test asserts no hits after `derived` and after its own collection; `test_the_transport_sentinel_is_actually_armed` pins the recorder. QA r2 (`agents/reports/2026-10-10-issue-134-defer-revision-gates-r2.md`): the r1 mutant now fails "the derivation reached the transport: ['get_component']" in strict and deferred mode, un-rebaselined and rebaselined; recorder replaced by `pass` → the armed witness fails |
| QA-134-r1-02 | Stage-1 QA r1, claim-only notes | "The banner prints before pytest's short summary and final line, not 'last' as the docstring says." | claim-only | none (prose) | n/a | Non-blocking — anchor: docstring, not served | working tree on `46100b78` | `fixed` — docstring corrected in the same batch as QA-134-r1-01 (one non-blocking pass) |
| CDX-134-r1-01 | Stage-2 repo Codex review r1, `cdx-review.o2tFRa` (STATUS: completed, head=df677533, dirty=false; archived) | "[P1] Clear CI state in deferred witness subprocesses — On GitHub Actions, `run_pytest()` preserves `GITHUB_ACTIONS=true`, so this explicitly deferred child is rejected by `pytest_configure()` with exit code 4 before collection … blocking every required CI run" | P1 | runtime behavior (the required CI suite fails) | DC-1 (2nd instance → structural fix) | **Critical** — anchor: source label P1 | `46100b78..df677533` | `fixed` — reproduced first (5 deferred witnesses failed under GITHUB_ACTIONS=true). Structural: the plugin reads the environment ONLY through `_env`, which refuses a name outside the plugin's variable tuple (every variable it reads or sets for a child); `child_env` drops all of the plugin's variable tuple. Witnesses: `test_the_plugin_reads_the_environment_only_through_its_funnel` (syntax-tree pin: `os.environ` only in `_env`/`child_env`; non-vacuity: an unknown name is refused), `test_a_deferred_child_runs_locally_even_when_its_parent_runs_under_ci` (parent CI → deferred child runs; explicit CI → still refused, exit 4). Under GITHUB_ACTIONS=true the 31 selected witnesses pass |


**DC-2 second-instance check (at QA-134-r1-01's row write).** The structural fix for DC-2 is the
marker bind itself, already in the tree; QA-134-r1-01 is a sibling the step-0 sweep did not
classify as a freshness comparison (it compared against the baseline as a side effect of a
transport property). Sibling re-sweep for the same mechanism — a behavioural assertion whose only
detection path is a committed-artifact comparison: the eight rows of the sibling sweep above were
re-read for it; the transport test was the one instance, now detecting through the sentinel's
record instead (non-vacuity: QA r2's recorder-removed mutant). Coverage claim: the artifact set is
closed (the plugin's closed artifact set, three files) and every reader of those files in `tests/` was
enumerated at step 0 by search over the file names and path constants.

**DC-1 second-instance structural fix (CDX-134-r1-01), sibling sweep** — every builder in the slice
that hands a child the parent's environment:
`tests/_revision_corpus.py::child_env` — fixed (drops all of the plugin's variable tuple);
`scripts/wave_gate.py::_pytest_env` — inherits the CI marker DELIBERATELY (the suite under CI is
CI) and drops the switch; `_render_env` — the golden render child loads no plugin, drops the switch;
`scripts/rebaseline_m12_12_inventory.py::_run` — its child (the emitter) reads no plugin variable and
is never reached under the switch (the script refuses first); `tests/test_wave_gate.py::_gate` —
the module's autouse fixture scrubs the CI marker and the switch. Coverage claim: the authority is
the plugin's own reads, and the syntax-tree pin makes a read outside the funnel a failure.

QA evaluations (Stage-1 loop): r1 (initial, 8 scenarios PASS, 1 Low + claim-only notes), r2
(affected re-run of QA-134-r1-01, 9 cells PASS, no new findings). No checkpoint due (2 < 3).

Stage-2 correction support QA (billed to the Stage-2 loop, not Stage 1): r3
(`agents/reports/2026-10-10-issue-134-defer-revision-gates-r3.md`) — affected re-run of
CDX-134-r1-01: R3-1..R3-5 PASS; control on pre-fix `df677533` under a CI parent: "3 failed, 2
passed"; with the fix "22 passed" / "5 passed"; the CI refusal intact (exit 4) top-level and for an
explicitly requested child. Stage-2 review evaluations: r1 (1 Critical, fixed). No checkpoint due.

## Checkpoints

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

## Deferrals

None.

## Evidence index

Collected run directories are archived under `docs/architecture/evidence/issue-134/` with
`index.jsonl` + `SHA256SUMS`, in the batch that collects them. The skeleton is created in the
Stage-1.5 commit with this file.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
