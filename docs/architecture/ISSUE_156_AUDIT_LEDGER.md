# Audit ledger — issue #156 (M12.18 Notify and recovery subprocess semantics, T4 + T5)

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

Plan of record: `.codex/plans/issue-156.md` — the attested Codex architect plan, attestation
`.codex/plans/issue-156.attest.json` (message sha256
`42f95604645969c3df29ac5cf64fda568b7d7768cc76c17387e4e5b484baaf2d`, thread
`01a074c1-b2d4-7840-ab43-70d3dc139c3f`, turn token 2, gate `architect`, collected
`ok:true`/`stopped:true`). Where the plan and this ledger disagree, the correction is recorded
here with its evidence and the plan is NOT silently re-authored.

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Issue | #156 — M12.18 Notify and recovery subprocess semantics (T4 + T5) |
| Step-0 baseline (`$BASELINE`) | `b07babeb3752d337ffa307c4c486e181e703a2c2` |
| Branch | `codex/issue-156` |
| Branch point | `dev` @ `b07babe` (closing commit of #155) |
| Baseline suite | 11555 passed, 19 skipped (11574 collected) — full non-KB, local `.venv` 3.12, `PYTHONPATH=src`, before any edit |
| Baseline manifests | 11574 required nodes (`tests/fixtures/wave_gate/test_nodes.jsonl`), 74 active goldens (`tests/fixtures/wave_gate/goldens.jsonl`) |
| Slice kind | behaviour-affecting (new canonical vocabulary, new lowering, changed served contract text) |

The collected count 11574 equals the recorded floor, so the floor is current on the branch point.

### Environment correction recorded at step 0

The Codex gates could not run at first: `~/.codex/config.toml` pins `model = "gpt-6-astra"`, and the
installed `@openai/codex` 0.151.0 rejected every turn with API 400 *"The 'gpt-6-astra' model
requires a newer version of Codex."* The first architect round was collected `--outcome failed`
(`declared_failed`, daemon confirmed stopped) and produced no plan. With owner approval the global
CLI was upgraded to 0.153.4 and the gate was re-run from a fresh session and run directory. No repo
file was involved; recorded here because a failed-then-rerun gate round must be visible in the audit
record rather than inferred from the surviving one.

## Loop roster (enumerated in advance, before the first correction)

Per `CLAUDE.md` the roster is fixed before any correction is applied. A gate not on this list cannot
mint a loop mid-run; adding one is itself a recorded checkpoint decision.

| # | Logical loop | Authority | Scope |
| --- | --- | --- | --- |
| 1 | Stage-1 QA | `boomi-qa-tester`, live through the public MCP tool boundary | the slice's affected scenarios |
| 2 | Stage-2 repo commit review | detached Codex review per `CLAUDE.md` §5b–5e | initial: `--base $BASELINE`; then each fix delta |
| 3 | Architect implementation review | `/codex-issue` §6 gate (`--gate review`) | implementation vs `.codex/plans/issue-156.md`; **capped at 3 evaluations** by `docs/architecture/COMPLETION_WORKFLOW_RULES.md` |
| 4 | Composite wave gate | `scripts/wave_gate.py` | full suite, golden manifest, determinism, wave-delta review, one live scenario per changed capability class |
| 5 | Terminal correction loop | only if a final non-blocking batch mutates the tree | that batch |

Pre-implementation live-oracle capture (architect plan §1 step 2 / §5) is **evidence provisioning**,
not a QA evaluation: it runs at the baseline tree with no source change and debits no loop.

## Plan corrections (recorded before the first correction is applied)

Claude's implementation plan is `.codex/plans/issue-156.claude.md`. It verified the architect plan
against the tree at `b07babe` and the following corrections OVERRIDE the architect plan. Each was
re-verified in the main thread before adoption; the architect plan is not silently re-authored.

| # | Architect said | Actually | Evidence (re-verified) |
| --- | --- | --- | --- |
| P1 | Retire `PROCESS_NOTIFY_CONFIG_INVALID` — it is "an advertised identity with no canonical producer" | **False premise.** It has live producers in `process_flow_builder.py` and is pinned by 14 assertions across `test_process_flow_builder_trycatch_dlq.py` (9), `test_process_flow_builder.py` (3), `test_integration_builder.py` (1), `test_schema_template_process_flow.py` (1). It is absent from `ERROR_TAXONOMY` because it is a LEGACY builder code, not a canonical one. **Keep it on the legacy path**; map only the canonical path to `PROCESS_IR_SCHEMA_INVALID` / `PROCESS_IR_SCHEMA_UNKNOWN_FIELD` / `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY`. Ship a producer-coverage test in place of the retirement. | `grep -n PROCESS_NOTIFY_CONFIG_INVALID tests/*.py src/boomi_mcp/errors.py` |
| P2 | Add the new semantic code to `semantic_validation/validation_policy.py`'s "closed diagnostic universe" | That module is a legacy-adapter **exemption registry** (`_EXEMPT_CODE`, `_POLICY_REGISTRY`). There is no closed universe there; a new code is non-demotable **by absence**. No edit; a negative test asserts it is not in `_EXEMPT_CODE`. | module read |
| P3 | Rework `lower_cfg_to_emission_plan` and `finalize_edges` for main-spine-first ordering | Both already produce the required order once CFG node order is right (`finalize_edges` sorts by `(source ordinal, local_ordinal)`; the plan assigns ordinals by `for node in cfg.nodes`). No edit. | `lowering.py` read |
| P4 | Rework `models/process_ir._walk_controls` for the recovery exemption | Its `try_catch` early return stays correct for the ancestor half; the exemption is needed and enforced only at the `body_capabilities` independent enforcement point. | `models/process_ir.py:3428` |
| P5 | Add Notify dispatch arms to `semantic_validation/effects.py` | `_replay_hazard` falls through to `None` for unknown kinds — Notify is neutral without code. Verify by test, do not add arms. | `effects.py` read |
| P6 | _(omission)_ | `_DISCRIMINATOR_TAGS` (`models/process_ir.py:2993`) is a hand-listed frozenset and must gain `"notify"` (and `"continue"`); the architect names only `_NODE_FACTS`. `_node_entries` raises `KeyError` for any `ProcessNodeV1` kind lacking a `_NODE_FACTS` row, so the projection hard-fails until both rows exist. | `process_ir_projection.py:279`, `models/process_ir.py:2993` |

## Scope decision — `ContinueNodeV1`

**IN SCOPE.** Reproducing `golden-000005`
(`connector_scoped_trycatch_notify_dlq_document_cache.xml`) canonically requires an explicit
no-emission try terminal: its region 1 protected path has **no closing shape at all** — it continues
into `shape4 map` — and every existing terminal (`stop`, `return_documents`) emits a shape. There is
no smaller design that reproduces those frozen bytes.

The decision is taken from the issue's acceptance criteria, not by preference: *"both legacy notify
goldens are reproduced byte-identically from canonical IR"* and *"Two sequential connector-scoped
regions compile with correct ordinals; mutation checks prove the updated plan invariants still
detect semantic drift"* are both explicit criteria, and in-scope item 5 names the region derivation,
ordinal allocation and plan-invariant work directly.

Two invariants block it today and are the critical path, both re-verified in the main thread:

* `invariants._check_control_depth` propagates the **incremented** depth to successors and treats
  `try_catch` as a control kind, while the ProcessIR v1 control-depth bound is 2 — so two sequential
  regions sit exactly on the bound and three fail. It must count ACTIVE nesting. (The bound's
  constant is named in `models/process_ir.py`; it is deliberately not spelled here, because the
  ledger scanner reads a code-shaped token as a diagnostic identity and that constant is not one.)
* `invariants._check_region_containment` requires a try edge's target `source_path` to start with
  `<tc>/try_body/` and recursively keeps the whole region inside it — an authorized continuation to
  `/body/steps/N` escapes.

## Findings ledger

One row per raw finding. Columns: source ID · verbatim summary · source gate / run dir /
attestation · original severity label · blocking class · defect class · derived tier + anchor ·
affected SHA/delta · disposition.

| ID | Summary (verbatim) | Source / run dir | Orig. label | Blocking class | Defect class | Tier (anchor) | Affected SHA | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `QA-156-oracle-r1-01` | "Your premise 'no source has changed yet' was true at my first command and false 16 seconds later — #156 implementation began editing the same worktree, including `process_flow_builder.py` at 03:52:02Z, 102 seconds after I emitted the oracle at 03:50:20Z. Had I run a bit later, the 'frozen legacy builder' oracle would silently have been the half-implemented one, with nothing failing." | `boomi-qa-tester`, pre-implementation oracle capture; `agents/reports/2026-09-05-issue-156-oracle-r1.md` | High | QA fixture provenance | dispatch-time-race (the capture ran against the shared worktree instead of an extraction) | **Standard** (no critical anchor: not secrets/security, data loss or mutation accounting, and the source label is High, not P0/P1/Critical) | `b07babe` (tree at capture) | `fixed` — the agent re-emitted all five artifacts from a pristine `git archive b07babe` extraction and got BYTE-IDENTICAL results, so the oracle is proven causally independent rather than assumed. The process defect is real and is recorded as a lesson: a pre-implementation capture must run from an extraction, not the shared tree. |
| `QA-156-oracle-r1-02` | "The freeze guard raised on `worktree_moved` … and, because it raises in `__exit__`, destroyed the runtime summary. `code_stable=True`, so the cells are not void — only the tree hash moved; I reconstructed the roll-up." | same | Low | _(none — agent instrumentation, not a served surface)_ | harness-teardown-masks-result | **Non-blocking** (outside every blocking class: agent-local tooling, nothing served to callers) | n/a | `fixed` by the agent in its own harness; no repo artifact involved. |
| `SELF-156-r1-01` | Emitter comment asserted "Reversing the two silently corrupts the binding" for the escape-then-substitute order. | self-review during implementation | n/a (self) | emitted XML validity | unverified-claim-in-served-comment | **Standard** (comment adjacent to emitted-XML logic; corrected before any gate ran) | uncommitted | `fixed` — the claim is FALSE: measured over 200,000 generated templates, both orders agree, because the token carries no apostrophe and neither it nor `{1}` can flip `_looks_like_json`. Comment rewritten to the true reason (oracle parity) and the equivalence pinned by `test_escape_then_substitute_and_the_reverse_agree_today`. |
| `SELF-156-r1-02` | First leak-control test for the recovery mixing exemption was vacuous — a Branch leg with a non-empty prefix is refused by the PREFIX rule before ancestry is consulted. | self-review during implementation | n/a (self) | capability reachability | vacuous-guard | **Standard** | uncommitted | `fixed` — rewritten with an empty-step leg so connector ancestry is the only rule that can fire; the test now fails if the exemption leaks. |
| `SELF-156-r1-03` | First cut of the containment relaxation was exploitable: forging `success_mode="continue"` onto a terminating handler silently widened its containment check and nothing objected. | self-review, mutation control | n/a (self) | emitted graph validity | permission-without-obligation | **Standard** | uncommitted | `fixed` — `success_mode` must now be EARNED: a handler declaring a continuation must reach a following region or `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` fires. Mutation control now detects it. |
| `SELF-156-r1-04` | The compiler placement mirror refused the serialized-region chain the parser accepted (`PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED`). | self-review, two-entry-point differential | n/a (self) | capability reachability | two-entry-point divergence | **Standard** | uncommitted | `fixed` — `_check_try_catch_placement` now renders the same chain grammar. Found because the byte-match alone did NOT catch it: `emit_process` does not run `validate_body_capabilities`. |

## Checkpoint records

Each checkpoint records: loop identity, window and cumulative evaluation numbers, current SHA and
dirty state, per-tier counts and breadth, new/resolved/recurring defect classes, trend evidence,
outcome and rationale.

| # | Loop | Window / cumulative | SHA (dirty) | Critical | Standard | Defect classes | Outcome | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| _(none yet)_ | | | | | | | | |

## Live evidence

Archive root: `docs/architecture/evidence/issue-156/` — a header-only `index.jsonl`
skeleton (no gate round is archived there yet; the Codex rounds append as they are
collected) plus `SHA256SUMS` covering every captured artifact, all tracked in git so the
archive is reproducible from a fresh clone rather than only present in this worktree.
Paths below are relative to that root.

| Capture | Provenance source | Artifacts | Runtime proof | Status |
| --- | --- | --- | --- | --- |
| B1 `catcherrors → processcall` | frozen legacy builder at `b07babe`, re-emitted from a pristine `git archive` extraction and byte-identical | `captures/oracle-graphs/B1_*` | see C1–C6 below | **SATISFIED** |
| B2 `catcherrors → notify → processcall` | same; no committed golden existed for this shape — the gap #156 fills | `captures/oracle-graphs/B2_*` | see C1–C6 below | **SATISFIED** |
| Runtime controls | forced failure via `docker stop aderant-mssql`; child nonce channel is an INDEPENDENT counterparty (`cds-mock`), so the switch cannot confound it | `captures/runtime/C1..C6*.json` | C1/C2 success control: parent-target 1, child 0. C3/C4 failure: child ran once. C5 abort control: failing child under `wait=true`/`abort=true` turns the PARENT to ERROR. C6 restore reproduces C1. | **SATISFIED** |
| Sequential-region retry isolation | — | — | fail only the second connector; prove the source runs once, the second connector exhausts its retries, the first recovery leg stays cold, the second child runs once | **OWED** — architect plan §5 makes this blocking for the `ContinueNodeV1` region-boundary claim; it is part of the Stage-1 QA engagement |

Platform semantics the capture pinned, which #156 must preserve: a handled catch leaves the
parent reading COMPLETE despite a SEVERE try-leg error (read the counterparty, never the
record); the Notify `{1}` binds the real caught error at runtime; the platform labels the call
`standalone start data, no return data`; and the child gets its OWN `ExecutionRecord` with
`executionType="sub_process"`.

## Gate evidence

| Gate | Round | Run directory | Reviewed SHA | Outcome | Teardown |
| --- | --- | --- | --- | --- | --- |
| architect plan | 1 | `/tmp/cdx-gate-architect.H2YfQr` (removed) | n/a | `failed` — `declared_failed`, model/CLI mismatch | confirmed stopped |
| architect plan | 2 | `/tmp/cdx-gate-architect.VzdlSK` (removed after copy) | n/a | `completed`, attested | confirmed stopped |
