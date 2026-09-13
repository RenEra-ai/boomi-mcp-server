# Audit ledger — issue #184 (M12.24 ProcessIR native document-sequence composition and non-returning staged workflows, bridge A)

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

Plans of record, each attested by the dispatcher-owned gate collector:

| Plan | Attestation | Message sha256 | Thread | Result |
| --- | --- | --- | --- | --- |
| `.codex/plans/issue-184.md` — the architect design plan | `.codex/plans/issue-184.attest.json` | `cc60610bf32775899ab1c67df4e863407043e55ac81aaaeace081be48c445980` | `01a098f7-b151-7d51-a9c3-4458351d4efe` | turn token 1, gate `architect`, `ok:true`/`stopped:true`, teardown `confirmed` |
| `.codex/plans/issue-184-amendment-1.md` — the passthrough entry and form-correct child contracts | `.codex/plans/issue-184-amendment-1.attest.json` | `7f8eb9f8556a3c32f8d09a78422a375443bae8d5eb1cc755a878efbfb7e11c88` | `01a0993f-6b0c-7f13-be60-7d48320fef46` | turn token 1, gate `architect`, `ok:true`/`stopped:true`, teardown `confirmed` |
| `.codex/plans/issue-184-amendment-2.md` — evidence corrections and the safe survivor replacement | `.codex/plans/issue-184-amendment-2.attest.json` | `f45fe41883f1dc82774243ea2088aa394bb8ebacd2006eaa8d44a244138f2ef3` | `01a09949-d54a-73c2-be84-e663c02104cb` | turn token 1, gate `architect`, `ok:true`/`stopped:true`, teardown `confirmed` |

Claude's implementation plan is `.codex/plans/issue-184.claude.md` (decisions D1–D12; its middle
file-by-file section was lost to a delivery failure and is covered by the design plan §3–§4). Later
plans override earlier ones where they say so. Where any plan and this ledger disagree, the
correction is recorded here with its evidence and the plan is NOT silently re-authored.

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Issue | #184 — M12.24 native document-sequence composition and non-returning staged workflows (bridge A) |
| Branch | `codex/issue-184` |
| Branch point | `dev` @ `cbab28ffc176ddb2378283cf7132eb9c37afc374` (closing commit of #158) |
| Pre-baseline commit | `73b9d3ddc8e547eba09b857275050fb336c06b59` — the frozen legacy both-sides oracle only (see below), required by A8 to precede the step-0 baseline; dark (no `src/` diff from the branch point, measured), full non-KB suite `12300 passed, 19 skipped, 19 warnings in 1073.10s (0:17:53)`, wave manifests `manifests ok (12319 required nodes, 79 active goldens)`; reviewed clean by loop L0 below |
| Step-0 baseline (`$BASELINE`) | `73b9d3ddc8e547eba09b857275050fb336c06b59` (printed by `git rev-parse HEAD` immediately after the oracle commit, before any behavioural edit) |
| Reference suite at the branch point | `12298 passed, 19 skipped, 19 warnings in 1001.00s (0:16:41)`, exit 0 — 12317 collected, equal to the recorded floor (`tests/fixtures/wave_gate/test_nodes.jsonl` header `minimum_collected` 12317); 78 active goldens — full non-KB, `python -m pytest tests --ignore=tests/kb -p no:cacheprovider`, local `.venv` 3.12, `PYTHONPATH=src`, detached worktree of the branch point, run to completion BEFORE any edit |
| Slice kind | behaviour-affecting (root and body grammar admissions, a derived per-path stream/profile proof, a lineage-carried cache-content profile fact, general document-property invalidation at stream-replacing reads, terminal-call prefix admission, a Data Passthrough entry form with form-correct child contracts, served-contract and revision changes) |

### Environment corrections recorded at step 0

1. **The architect gate sessions were told not to call Boomi MCP tools.** A Boomi MCP call parks a
   headless Codex turn on an elicitation it cannot answer; every brief grounded the plan in source
   reads and quoted documentation. All three plan turns completed on their primary prompts.
2. **The branch-point predicates were re-measured in the main thread, independently of the plans.**
   Every row of the issue's "Current predicates" table reproduced exactly — code, pointer and phase —
   through `parse_process_ir_v1` and `compile_process_ir_v1` against a pristine `git archive`
   extraction of the branch point. The probe and its output are archived under
   `docs/architecture/evidence/issue-184/predicates/`.
3. **No Boomi MCP server is connected to this session, and the local docs knowledge base exists only
   in the deployed container.** Platform facts were verified against the official help.boomi.com
   pages directly and quoted verbatim below.

## Loop roster (enumerated in advance, before the first correction)

| # | Logical loop | Authority | Scope |
| --- | --- | --- | --- |
| L0 | Pre-baseline oracle commit review (dark) | detached Codex review per `CLAUDE.md` §5b–5e | the oracle commit only, `--base cbab28ffc176ddb2378283cf7132eb9c37afc374`; run `cdx-review.w3jzMM` collected `STATUS: completed`, `SCOPE: branch diff against cbab28ffc176ddb2378283cf7132eb9c37afc374 (cbab28f) head=73b9d3ddc8e547eba09b857275050fb336c06b59 dirty=false`, no findings; archived |
| 1 | Stage-1 QA | `boomi-qa-tester`, live through the public MCP tool boundary | each admitted form separately, both child entry forms, the combined two-leg examples, the `EVAL-155-02` composite execution, and the named negative neighbours |
| 2 | Stage-2 repo commit review | detached Codex review per `CLAUDE.md` §5b–5e | initial: `--base $BASELINE`; then each fix delta |
| 3 | Architect implementation review | `/codex-issue` §6 gate (`--gate review`) | implementation vs the plans of record; **capped at 3 evaluations** by `docs/architecture/COMPLETION_WORKFLOW_RULES.md` |
| 4 | Composite wave gate | `scripts/wave_gate.py` | full suite, golden manifest, determinism, wave-delta review, one live scenario per changed capability class |
| 5 | Terminal correction loop | only via a recorded roster-addition checkpoint | that batch |

Two activities debit no loop because they validate nothing the slice changed: the pre-baseline
legacy-oracle capture (run from a pristine extraction of the branch point with no source change),
and the pre-implementation live evidence round `e0` (captures of platform behaviour from
causally independent fixtures, run from the branch-point worktree, feeding the admission rows).

## Pre-baseline legacy-oracle capture (A8)

A8 defines the mapped both-sides form by the legacy renderer's spine and requires the oracle to be
frozen before the step-0 baseline. None of the six existing dynamic-path goldens binds more than one
connector, so none is an oracle for it.

| Field | Value |
| --- | --- |
| Producer | `ProcessFlowBuilder.build(config, name="Dynamic Path Both Sides Golden", folder_name="Golden/Fixtures")` — the legacy renderer, never the canonical compiler |
| Config | the attested design's frozen config (`.codex/plans/issue-184.md` §3): REST GET source whose path composes from the run-supplied process property `seed_id`; map `MAP-UUID`; REST PATCH target whose path reads element 3 of `PROFILE-REQUEST` |
| Validator | `ProcessFlowBuilder.validate_config(config, depends_on=[], allow_rest_source=True)` returns None (measured) |
| Executed from | a pristine `git archive` extraction of the branch point, `.venv` symlinked, import origin asserted inside the extraction |
| Emitted spine (measured) | `start → documentproperties → connectoraction(GET, 1 dynamic Path property) → map → documentproperties → connectoraction(PATCH, 1 dynamic Path property) → stop` |
| Bytes / sha256 | 4471 / `633a197f3b67a0773a828acb378451d64183ca63a8c5ef60ddb095bbc053aede` |
| Determinism | two builds compared equal at capture |
| Archive | `docs/architecture/evidence/issue-184/oracle/` — capture script, input, output bytes, manifest |
| Registered as | `golden-000079` (`dynamic_path:both_sides`, owner `#184`, `transitional_oracle`) in the oracle commit |
| Provenance class | **legacy-oracle parity capture** from a tree causally independent of the implementation under test |

### Owner decision recorded at step 0 — both child entry forms

The implementation plan's D3 asked whether a child invoked by a terminal `process_call` receives
its caller's documents, because every canonical root emits a scheduled No Data Start. Verified
against official Boomi documentation (the Process Call step page): for a non-passthrough subprocess
"Documents are not passed into the subprocess" and "The parent process calls a new instance of the
subprocess for each document that reaches the Process Call step"; a passthrough subprocess receives
"any documents that reach the Process Call step as a group" in "one instance of the subprocess".
The #156 archive agrees: its No Data recovery child got its own execution record, one inbound
document per invocation.

The question was put to the owner. Decision, verbatim: "Well, we need to be able to support both
cases - most use of subprocesses use pass through but depending on logic it could be no data and in
this case we see the sub process as a separate in log, which could be useful for some cases, check
docs on this topic and apply what should we actually have for this case." #184 therefore supports a
Data Passthrough child and a No Data child, each with its documented semantics; the design is the
attested amendments 1 and 2.

### Verification of the amendments' load-bearing claims (main thread, before adoption)

Each claim was checked against its cited source, not accepted from the plan's prose.

| Claim | Cited source | Verdict | Evidence |
| --- | --- | --- | --- |
| a passthrough child must be called with wait=true | help.boomi.com Process Call step | **confirmed on re-fetch** (the first fetch wrongly reported it absent) | procedure step 4: "If the process being called is a passthrough process (Data Passthrough is selected in its Start step dialog), this check box cannot be turned off." The first fetch asked a summarizing model whether such a sentence existed and it answered "no"; amendment 2 disputed that, and a fetch reproducing the numbered procedure verbatim found it |
| passthrough reporting folds into the parent log; a No Data child gets its own execution record | same page | **confirmed** | "Process Reporting includes information about each subprocess as a step within the parent's process log. There are no separate execution records or logs for the subprocesses." / "Process Reporting contains a separate execution record and process log for each subprocess." |
| parent-set dynamic document properties are used by children receiving those documents | help.boomi.com Dynamic Document Property Examples | **confirmed on re-fetch** (the first fetch wrongly reported the page silent on parent/child) | the "Abstract profiles" example describes a shared child process that works provided each parent process sets the necessary dynamic properties before passing documents to it. Documented behaviour; the canonical bound-path use across the boundary is still unmeasured, so amendment 2 keeps it fail-closed until capture `cap184-passthrough-ddp-handoff` attests it |
| process properties are shared parent to child and child to parent after completion | help.boomi.com Process Property components | **confirmed** | "a process property value that is accessed or set in a parent process can be retrieved later in a child process. The opposite is also true…" |
| a document cache is shared between parent and child within one execution | help.boomi.com Document Cache components | **confirmed** | "A document cache can be shared among parent and child processes. You can add documents to the cache in the parent process, and those documents are available in any child processes." |
| the passthrough option bytes come from a UI-built pre-baseline capture | `docs/architecture/evidence/issue-175/stage1-qa-round-4.md` | **confirmed** | records the capture as "a UI-built live capture frozen 2026-07-03 — causally independent of the code under test" |
| the replacement survivor's bytes can come from `cap155-e1-source-dynamic-path` | `docs/architecture/evidence/issue-155/captures/cap155-e1-source-dynamic-path/` | **confirmed** | the directory holds `stored_process.xml`; `execute.json` supplies the process property `key` = `AC-0001`; the counterparty log reads `"GET /admin/cdscm/api/v1/clients/AC-0001 HTTP/1.1" 200 OK` and `"PATCH /admin/cdscm/api/v1/clients/AC-0001 HTTP/1.1" 200 OK` |

Code references in amendment 1 were checked for existence, not assumed: all fourteen test modules
its test table names exist; the execution-profile behaviour oracle it says must widen is
`authoring/contract.py` `_execution_profile_behaviour_oracle`; the asynchronous-ordering checks it
extends live in `semantic_validation/effects.py`; the reviewed placement prose table and the child
inspectability kind sets it cites exist in `authoring/process_ir_projection.py` and
`authoring/process_ir_effects.py`.

The two disputed citations and the survivor resolution were taken back to the architect as a dispute
round. `codex-drive start` refuses `--resume` together with a gate prompt hash, so the round ran in a
FRESH gated architect session with amendment 1 inlined verbatim by `cat` (never retyped) rather than
as an unattested resumed turn; its attested delta is amendment 2.

### Data Passthrough Start provenance source (pre-baseline, platform-authored)

`tests/fixtures/live_xml/m11/` holds four live captures committed at `e512c24` (2026-07-02, #119 census),
long before this slice's baseline, whose Start is `<passthroughaction/>`:
`process_setproperties_ddp_dpp_current_crossref.xml`, `process_cache_branch_load_remove.xml`,
`process_dpp_profile_decision_flow.xml`, `process_doccacheretrieve_loadalldoc_variant.xml`. Their
`<process>` open tag reads `allowSimultaneous="false" enableUserLog="false" processLogOnErrorOnly="false"
purgeDataImmediately="false" updateRunDates="false" workload="general"` (measured on two of the four),
with no `stopProcessingIfZeroDocuments` attribute. Provenance class: **live capture of a
platform-stored process**, causally independent of the implementation under test. Whether the
platform DELIVERS a caller's documents to such a child is not in these captures and is owed to
live evidence (`e0`) and Stage-1 QA.

### Pre-implementation sweeps (scratch worktrees of the branch point, never the working tree)

| Probe | Patch | Full non-KB suite | Non-vacuity witness |
| --- | --- | --- | --- |
| General document-property invalidation at stream-replacing reads (A5) | the general lattice also drops document-scoped keys where the property-survival table does not record `survives`, keeping keys the replacing node itself writes | `12298 passed, 19 skipped, 19 warnings in 1037.97s (0:17:17)` — no existing test changes | the issue's stale-read probe becomes refused (classified `PROCESS_IR_SEMANTIC_LINEAGE_DDP_SCOPE_INVALID` at `/body/steps/4`; the implementation must classify it read-before-write) |
| Absent-stream refusal for map and cache-put consumers (A4) | the per-path walk runs on binding-free documents too and refuses a map or cache put with no producer upstream | `12298 passed, 19 skipped, 19 warnings in 1046.30s (0:17:26)` — no survivor changes | both absent-stream probes become `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`; the staging skeleton still compiles |

So the A5 same-slice fixture sweep set is EMPTY at the branch point (measured, not assumed), and A4
breaks no survivor. The patches and witness outputs are recorded in
`docs/architecture/evidence/issue-184/predicates/SWEEPS.md`.

## Plan corrections (recorded before the first correction is applied)

| # | Plan said | Actually | Evidence |
| --- | --- | --- | --- |
| C1 | the design plan: the universal property-profile proof conflicts with active survivor `golden-000072`, which blocks completion until an initial-document authority or an acceptance-policy amendment exists | resolved by amendment 2 without a policy amendment: under the documented No Data semantics the survivor's first writer reads a profile element from the single empty start document, so its bound GET path gets an empty segment and addresses a different resource. It is not a VALID survivor, which is all A9 protects. A profile-valued source on the empty entry stream of a scheduled root is refused `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH` at `/body/steps/0/source_values/1/profile_ref`; `golden-000072` is retired through the manifest's active-to-tombstone transition and replaced by a safe survivor whose path composes from a run-supplied process property, its expected shapes cut from `cap155-e1-source-dynamic-path` | `tests/fixtures/process_ir/issue155/source_dynamic_path_profile.json` (profile source at index 1); the precedent in `tests/fixtures/process_ir/issue155/PROVENANCE.md` (the canonical chain refuses the unsafe legacy DDP path shape and freezes a safe equivalent); the Start step page ("a single empty document is forwarded through the process flow") |
| C2 | the implementation plan D1: a profile writer on the untouched entry stream records an entry requirement, keeping `golden-000072` unchanged | superseded by amendment 1's entry model: a scheduled root's entry is the single EMPTY document (a profile read there is refused, C1); only a Data Passthrough root's entry carries caller documents, and only there does an entry-position profile consumer define a requirement a caller discharges | amendment 1 §2; the Process Call and Start step pages |
| C3 | the design plan: no new entry form; the implementation plan D3: escalate if a No Data child receives no documents | superseded by the owner decision: a third entry form (Data Passthrough) is added, with form-correct child contracts for both forms | the owner decision above |
| C4 | amendment 1: the catch-body non-notify prefix refusal keeps its `/steps/<offender>` pointer (implementation plan D9) | A6 requires the residual prefix refusal at `/…/terminal`; amendment 1 adopts `/…/terminal` and this ledger follows A6 | issue #184 A6 text |

## Defect-class ledger (a class is a (mechanism, runtime-authority) pair, assigned at reconciliation)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-EVAL-155-02 | Inherited — `docs/architecture/ISSUE_155_AUDIT_LEDGER.md` row `EVAL-155-02` (#155 Stage-1 step-0 self-measurement at `9860842`, no gate run); adopted by #184's "Adopted from #155" placement record | "The issue's "a dynamic-path binding on the source AND one on the target" in one document is only partly expressible: the mapped both-sides composition the legacy chain emits today is refused at parse time by the root map-adjacency rule, so it is unrepresentable on the canonical chain" | none (self-recorded scope finding) | capability reachability | none (scope finding) | Standard — anchor: no source label and no critical blocking class; the #155 row records Standard | inherited at `cbab28f` | *(open — the expressibility half is discharged `fixed` only when A8 holds on the final tree: the canonical spelling compiles through the public route, emits `<shapes>` byte-identical to the frozen oracle, and executes green once live; #159 keeps the caller transfer and #160 verifies both before deletion)* |
| INH-EVAL-155-02a | Inherited — `docs/architecture/ISSUE_155_AUDIT_LEDGER.md` row `EVAL-155-02a` (revision of `EVAL-155-02`, raised by a served-surface audit at `2c1c9c7`) | "Same finding; the DISPOSITION claimed the enumeration was recorded in BOTH issue descriptions, and it was recorded in neither" | none (unchanged) | capability reachability (unchanged) | none | Standard (unchanged) | inherited at `cbab28f` | *(open — discharged together with INH-EVAL-155-02, including the same-pass #159/#160 description edits the issue lists)* |

**Supersession map** — `INH-EVAL-155-02a → INH-EVAL-155-02`.

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` · `deferred`
(issue, reason class, placement). An original label is never edited — a revision is a new dated line
with the original retained.

## Checkpoints (written in flight at every third evaluation of each loop)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

## Deferrals

Pointer-only — reason class, placement and lineage live on the finding row and in the filed issue.

## Evidence index

Collected run directories are archived under `docs/architecture/evidence/issue-184/` with
`index.jsonl` + `SHA256SUMS`, in the batch that collects them. The attestation scanner checks every
archive that has an `index.jsonl`: `SHA256SUMS` must cover exactly the on-disk archive (minus
itself) AND agree with the git index. So the skeleton created with this ledger covers the
pre-baseline `oracle/` capture and the `predicates/` measurements, and every later capture or
collected run is added to `SHA256SUMS` in the SAME commit that adds its files — an archive file
never sits uncommitted or unlisted across a validation boundary.

| Archived item | Path | Kind |
| --- | --- | --- |
| Legacy both-sides oracle capture | `docs/architecture/evidence/issue-184/oracle/` | pre-baseline evidence |
| Branch-point predicate probe and pre-implementation sweeps | `docs/architecture/evidence/issue-184/predicates/` | pre-implementation measurement |
| L0 pre-baseline oracle commit review | `docs/architecture/evidence/issue-184/commit-reviews/cdx-review.w3jzMM/` | collected commit review |

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
