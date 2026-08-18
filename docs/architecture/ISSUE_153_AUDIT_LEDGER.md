# Audit ledger — issue #153 (M12.15)

Instantiated at Stage-1 step 0 per `docs/architecture/templates/AUDIT_LEDGER_TEMPLATE.md`, and
committed — with its evidence-archive skeleton — in the Stage-1.5 baseline commit, before the
first Stage-2 round. Conventions inherited from #152's end state apply from row one.

Durable evidence archive: `docs/architecture/evidence/issue-153/` — every review round cited below
is present there as the collector's own artifacts (`commit-reviews/` for the repo's Stage-2
commit-review gate, `architect-reviews/` for the §6 architect gate), indexed by
`evidence/issue-153/index.jsonl` and hash-verified by `SHA256SUMS`. No round is cited here that is
not archived there, and no archived round is uncited.

Slice: **M12.15 — Canonical ProcessIR component materialization and direct apply (T1)**
Branch: `codex/issue-153`
Driver: `/codex-issue 153` (main-thread mode — this repo has no `.claude/workflows`, so the wrapping
pipeline runs `CLAUDE.md`'s own workflow rather than a composable Workflow engine).

## Baseline (Stage-1 step 0)

- Issue: #153 — M12.15 — Canonical ProcessIR component materialization and direct apply (T1)
- **Step-0 baseline SHA: `9f19aad5b280d58c02ef5cd840ff150d0193c1dd`** (== `origin/dev` at slice
  start; the tip #151 closed on)
- Slice kind: **behaviour-affecting** (it removes an apply refusal and adds a live mutation path)
- Artifact trust boundary:
  - **Created and owned by this slice** — the three new models (`ProcessComponentEnvelopeV1`,
    `ProcessAuthoringUnitV1`, `ProcessComponentMaterializationPlanV1`), the neutral
    `ProcessComponentMaterializer`, the relocatable plan fingerprint, the apply-time mutation
    attestation record, and the wave-gate plan-fingerprint provider registration.
  - **Consumed from outside** — the existing apply orchestration in
    `categories/integration_builder.py` (routing, preflight, dependency ordering, collision
    handling, create/update, preservation, verification), the legacy
    `process_flow_builder.py` arm (which stays live as the parity oracle until #160), the
    `compiler/process_ir` compile + emit chain, and the Boomi platform's own responses
    (server-assigned ids, folder attributes, readback bytes).
  - A finding that hardens a self-owned artifact gets a boundary verdict at reconciliation rather
    than a fix batch by default; a finding against a consumed artifact is in scope only where this
    slice changed its inputs.
- Expected defect classes (pre-enumerated from prior slices so a second instance triggers
  structurally ON ARRIVAL):
  - hand-enumeration shadowing a derivable authority (#149, #151, #165, #173);
  - hand-modelled platform behaviour in served text (#144, #151);
  - a guard whose expectation derives from the thing it guards, so it is vacuous (#149's derived
    gate, #151's `cls.__module__` witness, #165's collector-derived control);
  - an unpinned hand-copy of a fact whose runtime authority lives elsewhere (#140, #151);
  - a hand-typed count or ordinal where the value is derivable (#165, #173).

## Loop roster (fixed BEFORE the first correction; a gate not listed here cannot mint a loop
## mid-run — a roster addition is itself a recorded checkpoint decision)

| # | Logical loop | Gate purpose / authority | Scope | Checkpoint window |
|---|---|---|---|---|
| L1 | **Stage-1 QA** | `boomi-qa-tester` through the public MCP tool boundary, live on the renera account | this slice | fresh (3, 6, 9 …) |
| L2 | **Stage-2 repo Codex commit-review** | detached `codex-drive review --base` + `commit-review-collect.mjs`; every round COLLECTED, never read from `wait` | this slice | fresh |
| L3 | **§6 architect implementation review** (additive, declared by the wrapping `/codex-issue` pipeline) | detached `codex-drive --gate review` + `gate-attest.mjs collect`, judged against `.codex/plans/issue-153.md` | this slice | fresh |
| L4 | **Composite wave gate** | full non-KB suite + every active golden-manifest entry + determinism/byte-identity + `scripts/wave_gate.py wave … --require-plan-fingerprint` | Wave 0C slice set | fresh |
| L5 | Terminal correction loop | ONLY via a recorded roster-addition checkpoint | — | — |

Declared order of the additive gates: L1 → L2 → L3, with L4 run as one composite evaluation before
landing. Supporting QA/review runs bill the loop that caused the correction; a Stage-2 fix's QA
re-run never debits L1.

## Plan artifacts

- Codex architect design plan: `.codex/plans/issue-153.md` (gate-attested; attestation copied to
  `.codex/plans/issue-153.attest.json`). `.codex/` is gitignored, so these are session artifacts,
  not tracked evidence.
- Claude implementation plan: `.codex/plans/issue-153.claude.md`.

## Defect-class ledger (a class is a (mechanism, runtime-authority) pair, assigned at
## reconciliation, revisable with the original retained)

| Class | Mechanism | Runtime authority | Instances (derived from rows) | Resolution |
| --- | --- | --- | --- | --- |
| DC-1 | a documented COMPLETENESS claim that the code does not enforce — a docstring asserting "every field", "excluded by construction", "fingerprints behaviour", or "non-vacuity", restated in prose beside an enumeration that is free to fall behind | the authority the claim is about: the model's own `model_fields`, the dataclass's own `fields()`, the runtime dispatch table `PROCESS_FLOW_BUILDERS`, the emitter registration table | S5-01, S5-02, S5-03, S5-04, S5-05, S5-06, S5-07, S5-09, S5-10 (9 — derived from the rows above) | **STRUCTURAL FIX applied** — see below |
| DC-2 | a consumer that hand-models the PARTICIPANT UNIVERSE of a spec as `spec.components` alone, or reads a participant's backing component without first asking whether it has one | the one participant set `_integration_participants(spec)` — `components` **and** `processes` — which is also the authority `_topological_order` and the dependency graph read | S7-01, QA-153-r1-01, QA-153-r2-01, QA-153-r2-02, QA-153-r2-06, CX1-01, CX1-11, CX1-12 (8 — derived from the rows below) | **STRUCTURAL FIX applied in the r2 batch; its sibling sweep was INCOMPLETE** (it enumerated `integration_builder.py` only) and was extended over `authoring/workflow.py` in the Codex round-1 batch — see below |
| DC-3 | a serialized document searched as TEXT, with every matching string treated as structural | the SCHEMA's own reference-bearing fields — `ComponentRefV1`, identified by its validator in the field metadata | CX1-05, CX1-06, CX1-10 (3 — derived from the rows below) | **STRUCTURAL FIX applied in the Codex round-1 batch** — see below |

### DC-1 structural fix (mandatory — nine instances, far past the second-instance trigger)

**What the instances had in common.** Every one was a prose claim of completeness sitting next to a
hand-written enumeration: the canonical material "covers the model" while hand-building ten keys from
thirteen fields; the preservation policy "projects the runtime constant" while reading 2 of 8 fields;
the emitter revision "fingerprints behaviour" while covering only declared columns; the witness is
"THE non-vacuity witness" while touching no consumer; the builder list covers "every process builder"
while hand-copying the dispatch table. In each case the prose was true when written and had no
mechanism keeping it true.

**The invariant replacing the enumerations** — every completeness claim is now DERIVED from the
authority it is a claim about, so the enumeration cannot fall behind:

| Claim | Was | Now derived from |
| --- | --- | --- |
| fingerprint covers the plan | hand-built 10-key payload | `covered_plan_fields()` = `model_fields` − `EXCLUDED_PLAN_FIELDS` |
| projection covers the policy | 2 of 8 fields read by hand | `dataclasses.asdict(PROCESS_PRESERVATION_POLICY)` |
| guard covers every process builder | hand-listed 3 classes | `PROCESS_FLOW_BUILDERS.values()` |
| emitter revision covers the registry | declared columns only | declared columns **plus** `__qualname__`, with the residual limit stated |

**Sibling sweep.** Every completeness claim introduced by this slice was re-read and either derived
or narrowed: the four rows above are derived; `emitter_revision`'s claim was NARROWED to what it
measures (bytecode hashing rejected with a reason — `co_code` differs between the CI 3.11 and local
3.12 interpreters and would report drift between two correct deployments); `EXCLUDED_ENVELOPE_FIELDS`
is now read by `canonical_plan_material` rather than described by it. No remaining claim in the
slice's new modules asserts completeness over a hand-written list.

**Non-vacuity witness.** `test_all_consumers_observe_one_object_so_a_change_cannot_reach_only_some`,
re-measured against a PRISTINE `ec015d5` extract with the shared module added and the three
hand-copies left in place: **5 tests fail there**, including the witness itself and the regex sibling
sweep. The version it replaces PASSED on that same tree — which is what made it vacuous, and is the
concrete case the new invariant excludes.

**Coverage claim, derived from the authority's full case set.** `covered_plan_fields()` returns all
11 non-excluded fields of `ProcessComponentMaterializationPlanV1`; the preservation projection
carries all 8 `PreservationPolicy` fields and all 11 `OwnedPath` fields (asserted against
`dataclasses.fields`, not a literal count); `_PROCESS_BUILDERS` equals the 3 distinct values of
`PROCESS_FLOW_BUILDERS` and grows with it.

### DC-2 structural fix (mandatory — five instances; the first two were instance-patched, which is
### exactly what let the third and fourth ship)

**What the instances had in common.** Before #153 a spec had ONE participant family, so
`spec.components` and "everything this build owns" were the same set and every consumer could model
the universe by iterating it. #153 added `spec.processes`. Each consumer that kept the old model
became a defect, and — decisively — each was fixed AT ITS OWN CALL SITE:

| Instance | Where | How it failed |
| --- | --- | --- |
| S7-01 | `_build_plan` + `_apply_plan` component lookups | `KeyError` for any spec carrying a root — found by reading, fixed by guarding those two sites |
| QA-153-r1-01 | `_apply_plan`'s `existing_ids` comprehension | `KeyError: 'existing_component_id'` — fixed by adding the keys to the canonical step |
| QA-153-r2-02 | `_build_plan`'s issue-#86 advisory, ~1000 lines from the others | the r1 fix ADDED `planned_action`, which is what activated a third unguarded lookup. The guard had been False by accident, never by design |
| QA-153-r2-01 | `preflight_typed_apply_v1` | the bundle carried the SERVED projection, whose roots are withheld — the universe was not merely unmodelled but actively emptied |
| QA-153-r2-06 | `_verify_build` vs `_authoring_build_provenance` | two halves of one verify modelling two different universes, so a healthy build reported drift |

Three guarded call sites, and the fourth read shipped. That is the signature the structural-fix rule
names: patching instances of a hand-model does not converge, because the next consumer is written by
someone who never saw the patch.

**The invariants replacing the enumerations.**

| Claim | Was | Now derived from |
| --- | --- | --- |
| "this step has a component" | `components_by_key[step["key"]]`, unconditional, at three sites | `_step_component(step, …)` — discriminates on the step's own `materialization` marker, returns `None` for a canonical root, and raises a NAMED `IntegrationDependencyError` (not a bare `KeyError`) for a genuinely absent component |
| "these are the build's participants" | `for comp in spec.components` in `_verify_build` | `_integration_participants(spec)` — the same generator `_topological_order` and the dependency graph already read |
| "the bundle's spec can build what its compile describes" | trusted by convention, and violated | `CompiledBundle.__post_init__` compares the spec's root keys against `compile_result.process_cfg`, an authority INDEPENDENT of the projection under test |

The discriminator is deliberately the step's marker and NOT membership in `components_by_key`:
membership would fail OPEN and silently skip a component step whose key really was missing, turning
a crash into a wrong answer.

**Sibling sweep.** Every read of `components_by_key` in `integration_builder.py` was enumerated
(`grep -n 'components_by_key\['`) and converted: lines 5924 and 7820 now call `_component_for_key`,
line 6958's advisory calls `_step_component`, and the ONE remaining subscript is the accessor's own
lookup. The sole assignment (the wrapper construction site) is untouched by design. `_verify_build`
was converted to `_integration_participants`, and `_authoring_build_provenance`'s `results`-based
walk now agrees with it by construction.

**Non-vacuity witness — and it caught a defect in its own batch.** The invariant is asserted on the
SOURCE by `test_no_unguarded_component_lookup_survives_in_the_apply_pipeline`, because this
repository's own record is that prose structural fixes recur and executable ones hold. On first run
it immediately failed on a bare `verify_components_by_key[participant_key]` introduced by the
`_verify_build` conversion **in the same batch** — a fourth instance, written by the very change
meant to end the class, and caught before it left the working tree. Mutation-controlled in both
directions: reintroducing `comp = components_by_key[key]` at line 7820 reddens it; restoring the
accessor greens it.

**Coverage claim, derived from the authority's full case set.** `_build_plan` emits exactly two step
kinds, and the step-kind union is the authority: a component step (`materialization` absent) and a
canonical root step (`materialization == "process_ir_v1"`). `_step_component` has a case for each,
and both are exercised — the canonical case by
`test_a_plan_step_naming_no_component_is_a_named_refusal`, the component case by every apply test in
the file. `_integration_participants` yields both participant kinds and `_verify_build` now has a
`declared_type` branch for each.

Second-instance check: run against this table AT ROW-WRITE TIME, and also when a second finding
lands in the same file/subsystem within a loop (mechanism-family question: "what single authority
do these hand-model?"). On the second instance the structural fix is mandatory in that batch — or
in the immediately-next dedicated batch when it touches dispatch, compiler authority,
materialization, or shared apply code — with a sibling sweep, a non-vacuity witness, and an
authority-derived coverage claim.

## Finding rows (one per raw finding; append-only; exactly one disposition each)

| ID | Source gate + run dir + attestation | Verbatim summary | Original label | Blocking class | Defect class | Derived tier (anchor inline) | SHA/delta | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| INH-DR-1 | inherited seed — issue #149's trigger-map row 1, absorbed by this issue (#153 body, "#149 linkage"); #149 is CLOSED, so this slice is where the row is discharged | "**1 — direct roots validated late** (`recipes/composer.py:268-317`; `direct_process_roots` currently has ZERO callers, which is why this was non-blocking for #145)" — execution slot: "**In scope of #153.** Parse/validate direct roots BEFORE composition begins; a malformed root must fail typed, never as a raw `KeyError`. Small (S)." | non-blocking at #145 (recorded there: zero callers) | **not** a blocking class at this baseline — see the trigger correction below | (an unenforced type annotation trusted as a contract, the caller's actual input) — 1st instance | Standard-equivalent — no critical anchor, and no blocking class at this baseline since the parameter has no callers; hardened anyway because the issue puts it in scope | step 4 delta | `fixed` — `_validated_direct_roots` parses and validates every root, and both keys, before any classification/patch/composition phase; 19 tests, mutation-controlled (preflight removed → 17/19 fail) |
| S2-01 | self/measure — step 2 (typed contracts), working tree | Adding `IntegrationSpecV1.processes` inlines the entire ProcessIR schema into the SERVED `IntegrationSpecV1` schema template: canonical length `11390 -> 61651` bytes, `$defs` `4 -> 49`. Six served artifacts move: `SS-CAPABILITY-CATALOG:authoring_contract`, the `AuthoringCompileResultV1` / `AuthoringPlanResultV1` / `AuthoringRequestV1` / `IntegrationSpecV1` schema templates, and `SS-SCHEMA-TEMPLATES:walked_surface_digest`. | n/a (self-found measurement) | machine-served schemas/contracts | n/a — mandated rebaseline, not a defect: issue #153 in-scope item 4 requires the field | n/a (no gate label; recorded so a reviewer can weigh the payload growth) | working tree | `fixed` (rebaselined). **Verified benign, non-vacuously** — see the measurement note below |
| S3-01 | self/measure — step 3 (wire reshape), caught by `test_the_diagnostic_never_echoes_an_authored_value` | Moving process roots INTO `IntegrationSpecV1` put the caller's authored ProcessIR inside a SERVED field for the first time. The clean-room fixture's sentinel value, planted in the authored root, appeared in the served plan result at exactly two paths: `.authoring_result.integration_spec_preview` and `.integration_spec`. Zero diagnostics leaked. | n/a (self-found; the existing clean-room guard caught it) | **secrets/security** | (an internal structure promoted into a served field without re-applying the serving redaction rule, ADR-001 §11 value-free results) — 1st instance | **Critical** — anchor: CLAUDE.md makes any validated secrets/security finding critical, regardless of source label | working tree, step 3 | `fixed` — `build_integration_spec_preview` now withholds the roots; guard added with a positive control |
| S3-02 | self/measure — step 3, caught by re-running `test_an_in_plan_reference_is_resolved_not_dangling` after fixing S3-01 | The S3-01 fix regressed reference resolution: `build_resolved_reference_summary` and `_materialization_gaps` were both fed `spec_preview`, so redacting the preview dropped every process participant and a `$ref` to a DECLARED root was reported as dangling — the precise distinction that summary exists to draw. | n/a (self-found) | runtime behavior | (one projection serving both the served echo and an internal computation) — 1st instance; the same root cause as S3-01 viewed from the other side | Standard — no critical anchor; caught before any commit, fixed in the same batch | working tree, step 3 | `fixed` — both call sites now read `normalized.integration_spec`; redaction lives only at the serving boundary |
| S10-01 | composite wave gate (L4), `scripts/wave_gate.py wave --base 9f19aad --require-plan-fingerprint` | `PYTEST_NODE_MISSING 6 required node id(s) are not in the collection; removing a required test needs an explicit manifest tombstone in the same change.` | n/a (gate diagnostic) | — (verification bookkeeping; non-blocking class) | n/a — a deliberate rename, not a defect | Standard-equivalent | step 10 | `fixed` — the six retired refusal tests were RENAMED when inverted, because names asserting `cannot_be_applied` on tests that now assert the opposite would be actively misleading. Tombstoned the six retired node ids and registered the six replacements; the rename is 1:1, so the active count is unchanged (asserted in the same edit, not assumed). |
| S10-02 | composite wave gate (L4), earlier run | `WORKTREE_DIRTY the gate changed the worktree; it must be read-only.` — `M docs/architecture/ISSUE_153_AUDIT_LEDGER.md` | n/a (gate diagnostic) | — (non-blocking) | n/a — operator error, not a code defect | n/a | steps 5 and 10 | `fixed` — the ledger was edited WHILE the gate was in flight, which this repository's memory explicitly warns against. **This happened TWICE** (step 5 and again at step 10), which makes it a recurring operator error rather than a slip, so it gets a MECHANISM and not a resolution to be careful: the gate is now run only from a tree with `git status --porcelain` empty, verified in the same command that launches it, and nothing is edited until it reports. Every substantive check in that run had passed (suite green, 60 goldens byte-exact, `plan fingerprint checked:2 case(s)`), but a `WORKTREE_DIRTY` result is not a pass and was not treated as one; the gate was re-run on a clean tree. |
| S6-01 | self/measure — step 6, caught by the plan's own two-oracle control | The byte-parity differential lifted `process_options` OUT of each committed golden and passed it straight back, so it never exercised `DEFAULT_PROCESS_OPTIONS`. A one-byte mutation of that constant reddened the golden corpus (46 rows) but left the differential GREEN — the plan's stated "if only one goes red, one of the two oracles is not wired" case. | n/a (self-found) | — (test-oracle integrity; non-blocking class) | (a differential whose expected value is derived from the same source as its input, the golden corpus) — instance of DC-1's family, caught by a control rather than by review | Standard-equivalent | step 6 | `fixed` — the differential now also reassembles with `process_options=None` whenever the golden carries the scheduled default, and asserts at least one golden exercised it; re-running the same mutant now reddens BOTH oracles |
| S7-01 | self/measure — step 7, found by READING the consumers rather than by a test | Extending `_topological_order` over both tuples made `execution_order` contain process keys, while two loops (`_build_plan`, `_apply_plan`) still did `components_by_key[key]` unconditionally — a `KeyError` for every spec carrying a `processes` entry. No test caught it because none exercised that path yet. | n/a (self-found) | runtime behavior | (a producer widened without its consumers, the one shared execution order) — 1st instance | Standard — no critical anchor; would have failed loudly, not silently | step 7 → fixed in step 9 | `fixed` — `_build_plan` gained the canonical planning branch (emitting a `materialization: process_ir_v1` step) before the component lookup; surfaced first as `AUTHORING_COMPILE_BLOCKED` from the component-plan lint, which is how it was confirmed |
| S9-01 | self/measure — step 9, surfaced by `test_compile_routes_through_the_dispatcher` | The new `$ref` -> `depends_on` requirement fired on the M12.11 fixture, whose process unit references four components and declared none. | n/a (self-found) | runtime behavior | n/a — the requirement is correct; the FIXTURE predates it | Standard-equivalent | step 9 | `fixed` — fixture declares its four dependencies; the clean-room fixtures got theirs DERIVED from the refs each IR actually uses (intersected with the components that fixture declares), never hand-typed |
| S9-02 | self/measure — step 9, surfaced by the clean-room discoverability test | The design brief published `authoring.typed_apply.process_materialization` in `process_ir_capability_gaps` UNCONDITIONALLY, and stripped `apply` from `typed_next_steps` — so after the capability flipped, the brief advertised a supported capability inside a list of gaps and told the caller apply was refused. | n/a (self-found) | machine-served schemas/contracts | (a served projection hand-modelling a capability state the registry owns, `AUTHORING_CAPABILITY_REGISTRY`) — recurrence of DC-1's family in served text | Standard — served contract text is blocking class | step 9 | `fixed` — the gap row is now conditional on the registry state, and the `apply`-stripping filter is DELETED rather than inverted (a conditional re-adder would re-model the same fact) |
| S5-01 | self-run adversarial verification (workflow `wf_71a55dc6-cd0`, agent `verify:relocatability`), step 5 | "The canonical material carries an account-bound literal Boomi component id through `envelope.process_extensions.connections[].connection_id`, so the same logical process fingerprints differently in two accounts." | **refuted the design** (verifier verdict `design_holds: false`) | machine-served schemas/contracts + mutation accounting | (a documented completeness claim the code does not enforce, the model's own field set) — **1st instance** | **Critical** — anchor: mutation accounting; a plan whose central promise is false would attest a build that differs per account | step 5 working tree | `fixed` — reproduced independently (bytes differed across two accounts, account id present in material), then refused at plan validation with `PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE` |
| S5-02 | same verification run | "The emission plan's account-independence is an unenforced caller convention, not an invariant of the plan. Real account ids flow into the covered material whenever a non-default resolver is used." | refuted | mutation accounting | same pair as S5-01 — **2nd instance** → structural fix (below) | **Critical** — same anchor | step 5 | `fixed` — `build_materialization_plan` now OWNS compilation and forces `placeholder_backed_symbols`, so a real-id emission plan is unrepresentable; measured: a real-id resolver now yields material byte-identical to the placeholder run |
| S5-03 | same verification run | "`preservation_policy_v1()` is a lossy hand-enumeration wearing a projection's docstring: it reads 2 of `PreservationPolicy`'s 8 fields and 1 of `OwnedPath`'s 11, so the field that decides whether live state is discarded (`OwnedPath.mode`) is not covered by the fingerprint at all." | refuted | apply/update preservation | same pair — **3rd instance** | **Critical** — anchor: apply/update preservation; two materially different policies fingerprinted identically | step 5 | `fixed` — projection is now `dataclasses.asdict` of the runtime constant, complete by construction; test asserts every runtime field reaches the plan |
| S5-04 | same verification run | "`build_plan_fingerprint_fields` computes the digest via `model_construct` (no validation), so the plan-level slot-canonicalizing sort never runs on the material it hashes — making a plan with unsorted slots impossible to construct." | refuted | runtime behavior | same pair — 4th instance | Standard — no critical anchor; fails closed rather than silently | step 5 | `fixed` — slots are sorted before the digest is taken, inside the owning builder |
| S5-05 | same verification run | "`frozen=True` is shallow: `emission_plan` is stored as a mutable dict, so the stored `plan_fingerprint` can be desynchronized from the plan's own material after validation." | refuted | mutation accounting | same pair — 5th instance | Standard — defeats tamper-evidence but only under in-process mutation | step 5 | `fixed` — the emission plan is stored as canonical JSON TEXT, which is immutable |
| S5-06 | same verification run | "`EXCLUDED_PLAN_FIELDS` is dead documentation and the canonical material is a hand-written enumeration of the model's fields, with no coverage test anywhere — a new plan field is silently uncovered by the fingerprint." | refuted | machine-served schemas/contracts | same pair — 6th instance | Standard | step 5 | `fixed` — `canonical_plan_material` now walks `covered_plan_fields()`, derived from `model_fields` minus the exclusion set, so the set is load-bearing and a new field is covered by default |
| S5-07 | same verification run | "`emitter_revision()` does not cover what an emitter emits. Replacing an emitter's `emit` callable outright leaves the revision byte-identical, defeating the stated purpose of recording it on the plan." | refuted | machine-served schemas/contracts | same pair — 7th instance | Standard | step 5 | `fixed` by **narrowing the claim to what is measurable**, not by overreaching: `__qualname__` is now covered (a REPOINTED emitter is caught); bytecode hashing was considered and rejected because `co_code` differs between CPython 3.11 (CI) and 3.12 (local) and would report drift between two correct deployments. The docstring now states this limit explicitly. |
| S5-08 | same verification run | "`compiler_revision` is environment-dependent: every authority import is wrapped in a bare `except Exception` that substitutes the literal string `unavailable`, so a deployment where one optional import fails produces a different plan fingerprint for identical logical input." | refuted | machine-served schemas/contracts | (pre-existing degradation-to-`unavailable` in `_compiler_revision`, #146's surface) — distinct pair, not this slice's | Standard — real, but the mechanism predates this slice and is owned by `authoring/contract.py` | step 5 | `not-validated` **as a #153 defect** — the behaviour is #146's and unchanged here; #153 only consumes the value. Recorded rather than dismissed: the wave gate cannot catch it (both identities run in ONE process), so it is a genuine cross-deployment relocatability hazard. Disposition is to raise it with the review gates rather than silently widen this slice's scope. |
| S5-09 | self-run adversarial verification (agent `verify:preservation-parity`), step 5 | "The test advertised as 'THE non-vacuity witness' is vacuous: it touches no consumer and passes when literally nothing references the shared constant. Its docstring claim 'Under the old three-hand-copy arrangement this test could not pass' is false." — MEASURED against a pristine `ec015d5` extract | **refuted** | — (audit-record integrity; non-blocking class) | same (documented claim the code does not keep) pair — 8th instance | Standard-equivalent; audit integrity is non-blocking from row one per the template | step 5 | `fixed` — rewritten to assert every consumer resolves to ONE object; **re-measured against a pristine pre-extraction tree with the shared module added: 5 tests fail there, including the witness itself**, which the old version did not |
| S5-10 | same verification run | "The sibling sweep is a whitespace-exact substring match, and `_PROCESS_BUILDERS` is a hand-enumeration disconnected from the runtime authority `PROCESS_FLOW_BUILDERS`. A fourth registered process builder carrying its own hand-copied PreservationPolicy passes all 7 tests." | refuted | capability reachability | same pair — 9th instance | Standard | step 5 | `fixed` — the builder set is derived from `PROCESS_FLOW_BUILDERS`, and the sweep is a whitespace-insensitive regex with three positive controls covering the spellings the literal match missed |
| S5-11 | same verification run | "The frozen reachability inventory gains a scan_contract row that did NOT exist at the ec015d5 baseline: `scan_contract.python_source_count: 209 -> 212`." | refuted | capability reachability | n/a — mandated rebaseline for three new modules | Standard | step 5 | `fixed` — inventory and the paired §11 markdown regenerated together; served drift verified confined (5 aggregate-driven artifacts, ZERO structural schema change: intent properties added/removed both empty) |
| S5-12 | same verification run | "Dead import left behind: `from ._preservation_policy import OwnedPath, PreservationPolicy` is now unused in that module." | refuted (cosmetic) | — (non-blocking) | n/a | non-blocking | step 5 | `fixed` — removed after confirming nothing re-imports those names from `process_flow_builder` |
| S3-04 | #149 reachability census (`tests/_m12_12_legacy_inventory.py --check`), step 3 | New census row: `process_kind_producer \| src/boomi_mcp/authoring/workflow.py \| build_resolved_reference_summary \| IntegrationComponentSpec(type='process')`. The first version of the process-participant projection built a real legacy component spec as an internal shim. | n/a (self-found; the #149 census caught it) | capability reachability | (a synthetic internal shim built from the legacy model the census enumerates, `_m12_12_legacy_inventory` process-producer scan) — 1st instance | Standard — no critical anchor; latent, no behaviour change, but it GROWS the legacy surface #160 must retire | working tree, step 3 | `fixed` — replaced with a purpose-built `_ParticipantView` exposing only the four read-only attributes the lookup consults; census row confirmed gone by re-running `--check` |
| S3-03 | self/measure — step 3, caught by `test_an_absent_process_ir_still_reports_missing_not_a_shape_error` | `units: Tuple[...] = Field(min_length=1)` made a request carrying exactly one unit with a malformed root report BOTH `missing` (correct) and `too_short` on `units` (wrong): `min_length` counts elements that VALIDATED, so a single failing element reads as an empty list. A caller who forgot one key was told to send a second unit. | n/a (self-found) | machine-served schemas/contracts | (a field-level constraint evaluated against post-validation state, pydantic's `min_length` semantics) — 1st instance | Standard — served diagnostic text is blocking class; no critical anchor | working tree, step 3 | `fixed` — non-emptiness moved into the field validator, which does not run when an element fails, so only the diagnostic naming the real mistake is served |
| S2-02 | self/measure — step 2, working tree | The eleven per-schema entries inside `SS-SCHEMA-TEMPLATES:walked_surface_digest` ALL moved, including `ProcessIRV1`, `SystemTopologySpecV1` and `validation_report`, which this slice does not touch. | n/a (self-found) | machine-served schemas/contracts | (aggregate revision embedded in every served response, `revision_binding.schema_revision`) — explains the movement; not a hand-model, so no structural-fix trigger | n/a — expected consequence of a derived aggregate | working tree | `finding-refuted` as a defect: the movement is the derived aggregate, not a schema change. Each untouched schema's OWN `schema_hash` is unchanged; only the shared `revision_binding.schema_revision` moved, and it is derived over all owned schemas by design (`authoring/contract.py`). |
| QA-153-r1-01 | L1 Stage-1 QA round 1, `boomi-qa-tester`, report `agents/reports/2026-08-17-issue-153-m12-15-stage1-r1.md`, live against `traininghlibbochkarov-JKIY2X` | Typed apply raised `KeyError: 'existing_component_id'` for every spec carrying process units; the direct-apply capability was unreachable at the public boundary. | **Critical** (source label) | runtime behavior · capability reachability | DC-2 — **2nd instance** | **Critical** — anchor: source label Critical (immutable) | `acdb793` → fixed in `d94273e` | `fixed` — the canonical step carries `existing_component_id`/`planned_action`, and `_apply_plan` runs `_execute_canonical_process` BEFORE the component lookup. Re-verified live in r2: the apply loop now reaches the canonical arm. **Note the instance-patch cost: adding `planned_action` is what activated QA-153-r2-02**, which is the evidence that made DC-2's structural fix mandatory. |
| QA-153-r1-02 | same run | The S3-01 withholding never took effect on the served response: the legacy component-plan echo is rebuilt from the normalized spec and overwrote the withheld preview ~50 lines later, so planted canaries appeared in every served plan and compile response. | **Critical** (source label) | **secrets/security** | (a redaction applied before a later overwrite reinstates the redacted data, the last write to `spec_preview`) — 1st instance | **Critical** — anchor: secrets/security is critical regardless of label | `acdb793` → fixed in `d94273e` | `fixed` — `_withhold_process_roots` re-applied AFTER the echo, idempotently. Re-verified in r2 by a decisive A/B on one run: as shipped **0** canary hits at plan and compile; with the withholding neutralised to identity, **6** hits at plan and **3** at compile, at r1's exact six paths. |
| QA-153-r1-03 | same run | `canonical_process_apply.py` was imported by NOTHING in `src/`; the relocatability validator and the published `PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE` code were unreachable from any tool call. | **High** (source label) | capability reachability · machine-served schemas/contracts | (a validator whose only trigger is a code path no served route constructs, the served route set) — 1st instance | **Critical** — anchor: source label High | `acdb793` → partially fixed in `d94273e`, completed in the r2 batch | `fixed` — the module is on the apply path (r1) **and** the rule is now decided at plan/compile before any write, via the single shared authority `envelope_relocatability_offenders` (r2). r2 re-verification found the r1 fix PARTIAL and the residue is carried as QA-153-r2-07 rather than closed here. |
| QA-153-r1-04 | same run | A duplicate component key was served as a raw pydantic `ValidationError` with no `error_code`, while its two sibling graph rules were served properly — one rule family, two serving behaviours. | **High** (source label) | machine-served schemas/contracts | (a named rule served without its name because it raises through a different arm, `errors.py`'s taxonomy) — 1st instance | **Critical** — anchor: source label High | `acdb793` → fixed in `d94273e` | `fixed` — `_named_error_code_from_validation` maps the pydantic `type` to the served code. Re-verified live in r2: plan and compile both serve `INTEGRATION_COMPONENT_KEY_DUPLICATE`, with a control (renaming the collision compiles clean). |
| QA-153-r2-01 | L1 Stage-1 QA round 2, `boomi-qa-tester`, report `agents/reports/2026-08-17-issue-153-m12-15-stage1-r2.md`, live against `traininghlibbochkarov-JKIY2X` | "the typed apply silently DROPS every ProcessIR root and reports a successful mutation" — `_success: true`, `mutation_status: "performed"`, "Applied … with 2 steps", zero process components, no warning naming the dropped root. | **Critical** (source label) | runtime behavior · capability reachability · **mutation accounting** | DC-2 — **4th instance** (projection variant) | **Critical** — anchor: mutation accounting; an envelope attesting a mutation that did not occur | `d94273e` | `fixed` — `CompiledBundle` now carries `internals.normalized.integration_spec`, and `__post_init__` refuses any bundle whose spec lacks a root its own `process_cfg` describes. Introduced BY the r1-02 fix, which is recorded rather than smoothed over: the withholding was correct and the plumbing was not, and a silent no-op is strictly worse than the loud crash it replaced. |
| QA-153-r2-02 | same run | "a second unguarded `components_by_key[...]` raises `KeyError: '<process key>'` for every spec whose roots reach `_build_plan`" — the issue-#86 design-doctrine advisory at `integration_builder.py:6958`; three such reads exist, the slice guarded two. | **Critical** (source label) | runtime behavior · capability reachability | DC-2 — **3rd instance** → structural fix | **Critical** — anchor: source label Critical | `d94273e` | `fixed` **structurally, not at the site** — see the DC-2 structural fix. Also fixes a second, quieter failure QA's finding implies: `_legacy_plan_echo` swallows any `_build_plan` exception, so the KeyError silently DISABLED the duplicate-connection/base-URL/folder/name lints on the typed route while reporting success. Witnessed by `test_the_component_plan_lint_still_runs_for_a_canonical_root`. |
| QA-153-r2-03 | same run | "`_execute_canonical_process` calls an undefined name — every canonical apply is a `NameError`": `_connector_metadata_from_components` is defined in `authoring/workflow.py` and never imported, and the error is re-served as `PROCESS_MATERIALIZATION_PLAN_INVALID` with an internal symbol name on the wire, after connectors are already written. | **Critical** (source label) | runtime behavior · capability reachability · machine-served schemas/contracts | (a server fault served under a caller-blaming contract code, `errors.py`'s subject-per-prefix split) — 1st instance | **Critical** — anchor: source label Critical | `d94273e` | `fixed` — the function is IMPORTED, not reimplemented (copying its four lines would have removed the crash and reintroduced the hand-model this slice is closing). The generic arm now serves `PROCESS_MATERIALIZATION_INTERNAL_ERROR` for a server fault and consults `_NAMED_VALIDATION_CODES` for a real pydantic refusal, so `PLAN_INVALID` again means what it says. |
| QA-153-r2-04 | same run | "`bind_symbols_to_applied_ids` demands an applied id for the root being created — single-root apply fails 100% of the time", and with that repaired a multi-root spec fails on the first root because the sibling root's symbol is equally unapplied. | **Critical** (source label) | runtime behavior · capability reachability | (an enumeration — "every symbol in the table" — standing in for a derived requirement set, the root's declared `depends_on`) — 1st instance | **Critical** — anchor: source label Critical | `d94273e` | `fixed` — the binder resolves the root's DECLARED dependencies (already enforced as a superset of what it can reference, since an undeclared `$ref` is refused at compile) and leaves other symbols on their placeholder. Made fail-closed rather than optimistic: `materialize_canonical_process_xml` refuses any artifact in which a placeholder or `$ref` token actually survived, so a reference the rule wrongly judged unreachable stops the apply instead of shipping `id-db_conn` into a component that looks applied. |
| QA-153-r2-05 | same run | "both attestations are computed and then DISCARDED on the success path" — served only on a partial-failure envelope; absent from `apply_result` and from the build record, so verify has nothing to compare against. | **Critical** (source label) | **mutation accounting** | (an accounting record produced on the failure path only, the apply loop's own accumulator) — 1st instance | **Critical** — anchor: mutation accounting | `d94273e` | `fixed` — serialized once and attached to both the apply envelope and the build record. Additive: the keys are absent (not null) when no canonical root took part, so a legacy build keeps exactly its five original record keys — asserted by `test_a_build_with_no_canonical_root_keeps_its_original_record_shape`. |
| QA-153-r2-06 | same run | "`verify` never covers a process root — and on the typed path reports FALSE DRIFT for a perfectly healthy build": `_verify_build` walks `spec.components`, `_authoring_build_provenance` walks `results`, so the root is recorded and never observed → `missing` → `AUTHORING_LIVE_DEPLOYMENT_DRIFT`. | **High** (source label) | runtime behavior · machine-served schemas/contracts · capability reachability | DC-2 — **5th instance** | **Critical** — anchor: source label High | `d94273e` (latent, masked by r2-01) | `fixed` — `_verify_build` iterates `_integration_participants(spec)`, so the root is existence-checked AND graph-verified like any other process. Witness round-trips the REAL emitted artifact as the live readback rather than a hand-written stub (a stub would be a second hand-model of the emitter's output, silently deciding what verify sees) and asserts `_success is True` on a healthy build. |
| QA-153-r2-07 | same run | "the relocatability refusal is still absent at plan/compile, fires only after components are written, and is served under the wrong error code" — the r1-03 residue, in three parts. | **High** (source label) | machine-served schemas/contracts · runtime behavior | shares r1-03's pair — **2nd instance** → resolved by derivation rather than a second guard | **Critical** — anchor: source label High | `d94273e` | `fixed` — (a) `_validate_processes` now decides it, so `plan` REPORTS it (`is_valid: false`, error row at the offending path) and `compile` REFUSES it, before any write; (b) that removes the partial write entirely; (c) the apply arm consults `_NAMED_VALIDATION_CODES`, so the served code is `PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE`. The rule itself is NOT restated — both consumers call the one `envelope_relocatability_offenders`, so a second opinion cannot drift from the first. |
| QA-153-r2-08 | same run | "refusal envelopes echo caller-authored values and internal plan material, and omit the typed-apply envelope fields" — a 16-character prefix of an authored value measured on the wire via pydantic's `input_value=`, internal plan state in a caller-facing error, and no `action`/`mutation_performed`/`mutation_status` on the blanket-handler arms. | **Medium** (source label) | machine-served schemas/contracts | (a framework's default error rendering serving caller content, ADR-001 §11 value-free results) — 1st instance | **Standard** — no critical anchor; source label Medium. Recorded deliberately: the *class* (secrets) is critical, but the echo is of the caller's own payload back to that same caller, which is a served-contract defect rather than a disclosure to a third party — and it is fixed in this batch either way, so the tier changes nothing operationally | `d94273e` | `fixed` — `_validation_error_message` renders `loc`/`msg`/`type` and drops `input`/`ctx`; `_decorate_refusal_route` gives apply-route refusals the fields the contract tells callers to read. **The first version of the regression test passed on the broken tree**: it asserted the canary's first 16 characters were absent while pydantic had clipped the leak at 15, so the probe sat just past the window. Corrected to 12 and re-controlled. |
| SELF-r2-01 | self/measure — found while correcting the stale prose QA-153-r2-* exposed, by reading `plan_authoring_request_v1`'s comment against the behaviour this slice ships | The component-plan lint's unexecutable-step finding was downgraded from error to warning for `intent_kind == "process_ir"`, justified by "a direct ProcessIR intent is plan/compile-only by design, so nothing would be built from it either way". This slice makes that premise false: the intent IS applied, its component plan IS built, and `_apply_plan` refuses on the same `error_` prefix — so compile issued a binding whose apply could not succeed, for precisely the intent kind that had just become appliable. | n/a (self-found) | runtime behavior | (a conditional whose justifying premise the same slice invalidated, the set of appliable intent kinds) — 1st instance | Standard — no critical anchor; fails at apply rather than silently, and no mutation is mis-accounted | r2 batch | `fixed` — the exemption is DELETED, not re-conditioned (the replacement condition would be "will this plan be built?", now unconditionally yes). **Nothing failed when it was removed except the two tests that asserted the stale premise**, which is the finding: `test_a_process_ir_compile_is_not_blocked_by_a_lint_it_can_never_apply` was inverted and renamed (node tombstoned, replacement registered) and `test_compile_routes_through_the_dispatcher` re-pointed at an appliable fixture. A control — `test_an_appliable_process_ir_compile_is_NOT_blocked` — was added so the inversion cannot be satisfied by a tree that simply blocks every ProcessIR compile. |
| SELF-r2-02 | self/measure — found by re-reading my own QA-153-r2-08(c) fix rather than by a test | Giving apply-route refusals the typed envelope fields made the blanket handlers assert `mutation_status: "none"` — computed from an envelope that carries no `results` at all. For an exception that escapes the apply LOOP that is a guess, and it is the one guess that must never be made: `none` reads as retry-safe, and a retry under `conflict_policy="clone"` duplicates whatever was already written. | n/a (self-found, in this batch) | **mutation accounting** | (an accounting value computed from an envelope that cannot contain the evidence, `_apply_plan`'s own progress) — 1st instance | **Critical** — anchor: mutation accounting | r2 batch | `fixed` — the apply loop raises `_ApplyExecutionError` on any escape and the handler serves `mutation_status: "possible"` with a reconcile-before-retry hint. Scoped to the LOOP, not the whole function: everything above it is preflight, and a spec that fails to parse has provably written nothing, so reporting `possible` there would be alarmist and equally wrong. The first attempt DID wrap the whole call and was caught by `test_an_apply_route_refusal_carries_the_typed_apply_fields` flipping to `mutation_performed: true` on a pure validation refusal. Witness: `test_an_apply_that_throws_mid_flight_does_not_claim_it_was_retry_safe`, controlled against the pre-fix tree. |
| QA-153-r3-01 | L1 Stage-1 QA round 3, `boomi-qa-tester`, report `agents/reports/2026-08-17-issue-153-m12-15-stage1-r3.md`, live against `traininghlibbochkarov-JKIY2X` | The r2-08 sibling sweep missed a third arm: the apply-escape handler renders `f"Integration builder failed: {exc.cause}"` — raw `str()` — while the same handler calls `_named_error_code_from_validation(exc.cause)`, which anticipates precisely the cause type whose `str()` carries caller payload. Its two siblings both use `_validation_error_message`. Proven live: a `RuntimeError` cause serves clean, a `ValidationError` cause puts `input_value=` on the wire. | **Medium** (source label) | **secrets/security** · machine-served schemas/contracts | shares r2-08's pair (a framework's default error rendering serving caller content, ADR-001 §11 value-free results) — **2nd instance** → structural fix | **Critical** — anchor: any validated secrets/security finding is critical regardless of source label | `8d54f1a` | `fixed` structurally — see below. QA is explicit that reachability with caller-authored content is **unproven** (no natural pydantic error escaped the loop across 31 envelopes), so this is a latent path with proven serving behaviour; it is fixed as critical anyway because the anchor governs, not the reachability. |
| QA-153-r3-02 | same run | The canonical partial-failure envelope hard-sets `error_code: None` while the real code sits one level down at `partial_results[key].result.error_code`; records `status: "updated"` for a step that provably did not mutate (the target id returns HTTP 400 "ComponentId … is invalid"); and drops the `step_result` the component arm lifts. | **Medium** (source label) | **mutation accounting** | (a per-step outcome recorded from the ACTION ATTEMPTED rather than from the platform's answer, the step result's own `_success`) — 1st instance | **Critical** — anchor: mutation accounting | `8d54f1a` | `fixed` — the canonical arm now lifts the step's own `error_code` and `step_result`, and derives `status` from whether the platform confirmed the write rather than from the action that was attempted. QA notes `mutation_status: performed` was already correct, so no caller was misled about the ACCOUNT — the defect is in the per-step record, which is what verify and a human reconciler read. |
| QA-153-r3-03 | same run | Independent measurement of the leak window: pydantic renders a HEAD-position value's first **15** characters, so the 12-character assertion is non-vacuous and a 16-character probe would indeed sit past it — but pydantic elides the MIDDLE and keeps BOTH ends, so a TAIL-position value leaks its last ~24 characters with no prefix at all. A prefix sweep is therefore position-dependent and scored 0 on the QA-153-r3-01 envelope, which does render `input_value=`. | **Low** (source label) | machine-served schemas/contracts | n/a — a defect in the TEST's predicate, not in served behaviour | Standard — no critical anchor; source label Low | `8d54f1a` | `fixed` — the guard is now the position-independent invariant `"input_value=" not in served`, which is also what makes the structural fix for r3-01 checkable. This is the second time this slice that a probe measured the wrong thing and reported clean; recorded as such rather than as a passing detail. |
| QA-153-r4-01 | L1 Stage-1 QA round 4, `boomi-qa-tester`, report `agents/reports/2026-08-17-issue-153-m12-15-stage1-r4.md` | The source guard added for QA-153-r3-01 enforces ONE SPELLING, not the invariant it names: its predicate matches `{exc}` f-string interpolation and misses `str(exc)`, `%` and `.format` — and `str(exc)` is the spelling already used at ~20 sites in the same file. Mutation-controlled against the committed blob, so the measurement is immune to the dirty worktree. | **Low** (source label) | machine-served schemas/contracts | shares r2-08/r3-01's pair — **3rd instance**, and specifically a defect in the STRUCTURAL FIX itself | Standard — no critical anchor; source label Low | `98f58e5` | `fixed` — the guard is now an AST check. Scope is derived rather than enumerated: an `except` arm is IN SCOPE when it can receive an exception this module did not raise (bare, or `Exception`/`BaseException`/`ValueError` — QA confirmed at runtime that pydantic's `ValidationError` subclasses `ValueError`), and a handler catching a type this repo defines is out of scope by construction, not by allowlist. Mutation-controlled against all four spellings QA named; each reddens it, the clean tree passes. Two open arms it found were converted (`_build_plan`'s `except ValueError`, `_verify_build`'s per-participant catch). QA's own note recorded: the `re.error` conversion buys the invariant, not a behaviour change, because `re.error.__str__` omits the pattern. |
| QA-153-r4-02 | same run | The `_apply_structured_update` PUSH arm serves no `error_code`, so QA-153-r3-02's lift has nothing to lift. "The asymmetry runs backwards to the stakes: the fetch arm (nothing written) is machine-classifiable, the push arm (a write may have landed) isn't." | **Low** (source label) | machine-served schemas/contracts · mutation accounting | (a named diagnostic on the low-stakes arm of a pair and none on the high-stakes one, the pair's own failure modes) — 1st instance | Standard — no critical anchor; source label Low. Recorded deliberately: the mutation-accounting CLASS is a critical anchor, but this row is the absence of a machine-readable label on an already-served failure, not a mis-stated mutation | `98f58e5` | `fixed` — the push arm serves `UPDATE_PRESERVATION_PUSH_FAILED`, so the canonical arm's lift has something to lift on the arm that matters most. |
| CX1-01 | L2 Stage-2 Codex commit-review round 1, run dir `cdx-review.xSupcS`, `STATUS: completed`, `SCOPE: branch diff against 9f19aad5b280d58c02ef5cd840ff150d0193c1dd (9f19aad) head=98f58e56fcde5846789ae7e6b31d2d43cae574e9 dirty=false` | "Resolve canonical process targets during planning … an update-by-name passes fail-fast and can fail only after dependencies have mutated, while a create does not honor `reuse`, `fail`, or `clone` for a same-name live process and attempts another create." | **P1** | runtime behavior · mutation accounting | DC-2 — **6th instance** (a canonical root denied the collision planning every component step gets) | **Critical** — anchor: source label P1 | `98f58e5` | `fixed` — the canonical branch resolves by name through the SAME `_resolve_existing_components` the component arm uses, and derives `planned_action` under the same policy rules (`reuse`/`clone`/`error_if_exists`, plus `error_missing_target` / `error_ambiguous_match` for an update). |
| CX1-02 | same round | "Keep clone policy on the create path … this line selects `update` merely because `target_id` is truthy. The canonical path therefore overwrites the existing process instead of suffixing the name and creating a clone." | **P1** | runtime behavior · **data loss** | (a policy branch selected by a value's truthiness rather than by the policy, `conflict_policy`) — 1st instance | **Critical** — anchor: data loss (it overwrites a live component the policy exists to leave alone), and source label P1 | `98f58e5` | `fixed` — a clone clears `target_id` and creates under a suffixed name, through `_CLONE_SUFFIX_FORMAT`, which is now the ONE clone-naming rule both participant kinds format through. The decision moved AHEAD of materialization so the plan describes what will actually be built, rather than renaming an artifact already fingerprinted under the original name. |
| CX1-03 | same round | "Bind the materialization fingerprint into compile … the caller's compile hash does not cover this plan's materializer revision, emitter revision, or preservation policy; after such behavior changes, preflight can accept the old compile binding." | **P1** | machine-served schemas/contracts · mutation accounting | (a binding that cannot observe the artifact it certifies, the artifact set the compile hash covers) — 1st instance | **Critical** — anchor: source label P1; a staleness check that cannot fire is a false assurance | `98f58e5` | `fixed` — `build_artifact_descriptors` now emits a `process_component_materialization_plan` fingerprint per root, through `build_materialization_plan` itself rather than a second implementation of it. The model already declared that `artifact_kind`; the slot existed and was unpopulated. Witness moves ONLY the materializer revision and asserts the compile hash moves. Cost recorded: each root now compiles twice, and the second compile is load-bearing — `build_materialization_plan` owns its compilation and forces placeholder-backed symbols, which is the S5-02 structural guarantee; reusing the outer compile would hand it a table it did not build. |
| CX1-04 | same round | "Mark pre-write refusals as non-mutating … a process-only request rejected here therefore reports `mutation_status="possible"` and tells the caller to reconcile despite having issued no mutation." | **P1** | mutation accounting | (an outcome inferred from a status vocabulary that does not distinguish the case, the step's own write attempt) — 1st instance | **Critical** — anchor: mutation accounting | `98f58e5` | `fixed` — pre-write refusals record `status: "refused"`, added to `_NON_WRITING_STEP_STATUSES`. `failed` keeps meaning "a write was attempted and did not succeed", which is genuinely unknown and correctly reads as `possible`. Witness asserts both directions. Note this is the exact complement of QA-153-r3-02, which pushed `status` toward `failed`; the two together give three distinct values for three distinct facts. |
| CX1-05 | same round | "Inspect structural references instead of arbitrary XML text … a process with component key `db` and message text containing `id-db` is rejected after its dependencies may already have been created even though every structural reference was correctly rebound." | **P1** | runtime behavior | **DC-3** (a serialized document scanned as TEXT, with any matching string treated as structural; authority: the schema's own reference-bearing fields) — **1st instance** | **Critical** — anchor: source label P1 | `98f58e5` | `fixed` — the guard parses the artifact and matches a placeholder only as a COMPLETE attribute value. A structural reference IS the whole value of an attribute; authored prose containing the same token is not. Exact attribute-value equality separates them without enumerating which attributes carry references, which would be a hand-model of the emitter. |
| CX1-06 | same round | "Reject unmaterializable process self-references early … The request therefore passes planning, retains its own placeholder, and fails during materialization after earlier dependencies may have been written." | **P1** | runtime behavior | shares CX1-05's family (a reference rule decided late, on the artifact, rather than at preflight on the document) | **Critical** — anchor: source label P1 | `98f58e5` | `fixed` — `_check_process_root_dependencies` refuses a root that references itself, with its own message, at plan time. Witness asserts the guard directly AND carries a control (the same envelope naming a real dependency is accepted), because the legacy component-plan lint also refuses this shape end-to-end and would otherwise mask the new guard. |
| CX1-07 | same round | "Attest the placement the update actually preserved … This attestation nevertheless records the requested envelope name as the resolved placement, producing a false mutation record." | **P1** | **mutation accounting** | shares QA-153-r3-02's pair (an outcome recorded from the INTENT rather than from what the platform received) — **2nd instance** → structural fix | **Critical** — anchor: mutation accounting | `98f58e5` | `fixed` — `resolved_placement` is read from the SUBMITTED bytes for an update (`applied_folder_name`), which for a read-merge-write are the merged result the platform actually received. The pair's structural rule is now explicit and applied at both instances: **an attestation field is derived from the submitted/observed artifact, never from the request** — the same rule `submitted_xml_digest` already followed, extended to placement and to per-step `status`. |
| CX1-08 | same round | "Preserve process attestations inside authoring provenance … A successful apply consequently returns non-empty top-level attestations alongside contradictory empty arrays in `authoring_provenance`, and `verify` later returns only the empty nested values." | **P1** | mutation accounting | (a record written to one of two places that both claim to hold it, `AuthoringBuildProvenanceV1`'s declared fields) — 1st instance | **Critical** — anchor: mutation accounting | `98f58e5` | `fixed` — `_authoring_build_provenance` takes the attestations and populates the model fields that were declared for exactly this and left at their defaults. This is the completion of QA-153-r2-05, whose fix attached them at the record root only. |
| CX1-09 | same round | "Reject malformed recipe extension bindings … The strict typed models therefore never see the malformed data, and canonical apply emits a process without the requested environment overrides where the legacy renderer would have refused it." | **P1** | runtime behavior · machine-served schemas/contracts | (a tolerant normalizer silently discarding what it cannot interpret, the legacy reader's own refusals) — 1st instance | **Critical** — anchor: source label P1 | `98f58e5` | `fixed` — a non-Mapping block and a non-Mapping connection/field entry now raise `AUTHORING_COMPILE_BLOCKED` with the offending path. `None` still normalizes to empty bindings, because "absent" is a legitimate input and only "present but uninterpretable" is the defect. |
| CX1-10 | same round | "Restrict dependency discovery to typed reference fields … This recursive walk classifies every string beginning with `$ref:` as a component dependency, including literal message text, script bodies, static values, and templates." | **P2** | runtime behavior | **DC-3** — **2nd instance** → structural fix | Standard — in a blocking class, no critical anchor, source label P2 | `98f58e5` | `fixed` structurally — see the DC-3 block below. |
| CX1-11 | same round | "Include process edges in the dependency summary … A process depending on four supporting components therefore returns an empty dependency summary even though the unified execution graph enforces all four edges." | **P2** | machine-served schemas/contracts | DC-2 — **7th instance** | Standard — source label P2 | `98f58e5` | `fixed` — `build_component_dependencies` walks both participant families, and is fed the INTERNAL spec. The edge summary is value-free (keys only), so the root withholding that protects authored bodies does not apply to it. |
| CX1-12 | same round | "Keep lifted recipe roots in topology validation … A recipe request whose topology relation references its composed process root therefore regresses from a valid process symbol to `TOPOLOGY_REFERENCE_NOT_FOUND`." | **P2** | runtime behavior · capability reachability | DC-2 — **8th instance** | Standard — source label P2 | `98f58e5` | `fixed` — `_validate_topology` projects symbols from both families. **This and CX1-11 are the evidence that the DC-2 sibling sweep was INCOMPLETE**: it enumerated `integration_builder.py` and stopped there, while the class spans every consumer of a spec's participant set, including three in `authoring/workflow.py`. Recorded rather than quietly folded in — an incomplete sweep is how a structural fix becomes another instance patch. |
| CX2-01 | L2 Stage-2 Codex commit-review round 2, run dir `cdx-review.RcEkZs`, `STATUS: completed`, `SCOPE: branch diff against 98f58e56fcde5846789ae7e6b31d2d43cae574e9 (98f58e5) head=517f31d32acadb2f13c117224aa5e22c10b487ed dirty=false` | "Use the bound policy for plan fingerprints … compile fingerprints a plan built as `\"reuse\"`, while `_execute_canonical_process` builds and attests one using the requested policy … so the newly served provenance certifies a different plan than the mutation executed." | **P1** | **mutation accounting** · machine-served schemas/contracts | (a fingerprint computed under a value the caller did not request, the request's own `conflict_policy`) — 1st instance; introduced BY the CX1-03 fix | **Critical** — anchor: mutation accounting | `517f31d` | `fixed` — the compile-time fingerprint uses `request.intent.conflict_policy`. One divergence is retained DELIBERATELY and documented: when a create collides under `clone`, apply materializes a renamed envelope, so its plan genuinely differs from the compiled one. Making them equal would require compile to resolve live collisions, which is precisely what would make the plan account-bound and destroy the relocatability the fingerprint exists to certify — so the difference is the correct signal that a clone occurred, not a defect. |
| CX2-02 | same round | "Handle multiple process matches on create … Apply consequently performs an ordinary unsuffixed create under `reuse`, `fail`, and `clone`, violating all three collision policies and allowing an unintended duplicate instead of refusing or cloning safely." | **P1** | runtime behavior · data loss | DC-2's family (a collision case the component arm handles and the canonical arm did not) — introduced BY the CX1-01 fix, which added resolution but only branched ambiguity for updates | **Critical** — anchor: source label P1 | `517f31d` | `fixed` — ambiguity is treated as a collision whatever the action: `clone` proceeds as `create_clone` (it creates something new either way), `reuse` and `fail` refuse with `error_ambiguous_match`. Mirrors the component arm's rule rather than inventing a second one. |
| CX2-03 | same round | "Reject malformed connections containers … For a present block such as `{\"connection\": [...]}`, `{\"connections\": null}`, or `{\"connections\": {}}`, this coalesces the malformed value to an empty iterable and returns empty bindings … recipe normalization still silently drops requested environment overrides." | **P1** | runtime behavior · machine-served schemas/contracts | **DC-1's family** — a hand-copied SUBSET of an authority's refusals, authority `extension_bindings_from_legacy_config`; introduced BY the CX1-09 fix | **Critical** — anchor: source label P1 | `517f31d` | `fixed` structurally — the normalizer no longer answers the tolerance question, it DELEGATES it. See below. |
| CX2-04 | same round | "Preserve the legacy empty-list no-op … The legacy `extension_bindings_from_legacy_config` explicitly accepts an empty list as a no-op, and this boundary promises to preserve every legacy-accepted input, so existing equivalent recipe configuration now fails instead of emitting empty overrides." | **P2** | runtime behavior | same instance as CX2-03 — the OTHER direction of the same hand-copy | Standard — in a blocking class, no critical anchor, source label P2 | `517f31d` | `fixed` by the same delegation. Recorded as its own row because it is the evidence that a hand-copy of an authority is wrong in BOTH directions at once: the same 38 lines rejected what the authority accepts AND accepted what it rejects. |
| CX2-05 | same round | "Register the new preservation error code … callers now receive `UPDATE_PRESERVATION_PUSH_FAILED`, but this patch defines it only as a local string and never adds it to the shared `boomi_mcp.errors.ERROR_TAXONOMY`." | **P2** | machine-served schemas/contracts | (a served stable code absent from the taxonomy consumers classify by, `ERROR_TAXONOMY`) — 1st instance | Standard — source label P2 | `517f31d` | `fixed`, but NOT as prescribed — recorded because the remedy differs from the request. Registering only the new member would have put an #153-owned code outside #153's three declared prefixes and broken the one-introducer-per-family biconditional `test_issue_153_owns_its_three_families_as_a_biconditional` enforces. The whole `UPDATE_PRESERVATION_*` family (4 codes) is registered instead, under **#45**, the issue `git log -S` identifies as introducing it — which closes the real gap (the family was never discoverable) rather than half of it. `FETCH_FAILED` is declared `retryable=True` in the named retryable set with its reason; `PUSH_FAILED` deliberately is not, because the merged document was submitted and a write may already have landed. |
| QA-153-r6-01 | L1 Stage-1 QA round 6, `boomi-qa-tester`, report `agents/reports/2026-08-18-issue-153-m12-15-stage1-r6.md`, freeze stamp `6fa44c4102cb/clean/9cd84129b28b` identical at entry and exit of all 20 scenarios | "`conflict_policy` silently degrades to CREATE for any name that was previously soft-deleted." A live process resolves to ZERO candidates after its name has been deleted and recreated, so the policy block never runs: `fail` does not fail and `reuse` does not reuse. | **High** (source label) | **mutation accounting** · runtime behavior | (a guarantee about an existing component decided by a lookup that cannot prove absence, the metadata query) — 1st instance | **Critical** — anchor: source label High | `6fa44c4` | `fixed` **as far as this slice can soundly fix it, and the remainder is stated rather than implied.** QA disproved timing as the cause (0/2/5/10 s; a fresh name resolves in ~3.9 s and `fail` refuses every time) — the discriminator is deletion history. The mechanism lives in the metadata query and is SHARED with the component arm; what this slice changed is that a canonical root's policy now rests on it. Refusing every create-by-name would break the ordinary case, so a create-by-name under `fail`/`reuse` that resolves to zero now carries a served warning naming the limit and the guaranteed alternative (`envelope.component_id`, which skips resolution entirely — QA confirmed GET-by-id is unaffected). Carried to QA r7 as a live question: is the recreated component returned by the query at all, and with what `current_version`/`deleted`? That determines whether a `show_all=True` + local-dedupe fix in `paginate_metadata` is correct, which would be the real repair and belongs where the mechanism is. |
| CX3-01 | L2 Stage-2 Codex commit-review round 3, run dir `cdx-review.tqN8HE`, `STATUS: completed`, `SCOPE: branch diff against 517f31d32acadb2f13c117224aa5e22c10b487ed (517f31d) head=6fa44c4102cb17c9fcc852f9bf5d7e4c5a5e9d66 dirty=false` | "Keep ambiguous updates from selecting an arbitrary target … `_execute_canonical_process` ignores `planned_action`, sees the update action plus that ID, and updates the first match, silently rewriting an arbitrary process." | **P1** | runtime behavior · **data loss** | shares CX1-02's pair (a policy branch applied without checking the action it is valid for, `conflict_policy`'s create-only semantics) — **2nd instance** | **Critical** — anchor: data loss, and source label P1 | `6fa44c4` | `fixed` — `create_clone` is restricted to `action == "create"`; an ambiguous update stays `error_ambiguous_match` with no existing id. Introduced BY the CX2-02 fix: mirroring the component arm's rule, I mirrored the OUTCOME (`clone` proceeds) without its GUARD (`clone` proceeds *on the create path*). Recorded as the second instance of the same pair because it is: both are a clone decision made from a truthy value rather than from the action. |
| CX3-02 | same round | "Include matched candidates in canonical ambiguity steps … the plan omits the IDs needed to disambiguate, and apply's fail-fast message reports that zero components matched because it defaults the missing list to `[]`." | **P2** | machine-served schemas/contracts | DC-2's family (a canonical step omitting a field its component sibling carries) | Standard — source label P2 | `6fa44c4` | `fixed` — the canonical step carries the same sanitized `candidates` shape (`component_id`/`name`/`folder_name`) component steps carry. No extra live metadata is fetched: the list is already in hand from the resolution the previous round added. |
| CX3-03 | same round | "Convert indexed extension fields into JSON pointers … replacing only dots emits `/process_extensions/connections[0]/fields[0]` … Consumers following the diagnostic path therefore look for nonexistent keys such as `connections[0]`." | **P2** | machine-served schemas/contracts | (a format conversion that handled one separator of two, the builder's bracket notation) — 1st instance; introduced BY the CX2-03 delegation | Standard — source label P2 | `6fa44c4` | `fixed` — `_json_pointer` splits on dots AND brackets, so `connections[0].fields[0]` becomes `/connections/0/fields/0`. Witness asserts no bracket survives in any served diagnostic path. |
| CX3-04 | same round | "Register the entire preservation error family … The merge engine also serves `UPDATE_PRESERVATION_XML_PARSE_FAILED`, `UPDATE_PRESERVATION_OBJECT_MISSING`, and `UPDATE_PRESERVATION_MERGE_FAILED` … while the new exact-set test codifies the incomplete catalog." | **P2** | machine-served schemas/contracts | **DC-1** — a hand-listed set asserted as complete, authority: what the emitting modules actually serve; introduced BY the CX2-05 fix | Standard — source label P2 | `6fa44c4` | `fixed` — all **seven** emitted members registered under #45. The sharper half of this finding is the second sentence: my own witness hand-listed four codes and asserted the set was exact, so it PINNED the incomplete catalog as correct. The test now DERIVES the expected set by scanning what `integration_builder` and `component_update_preservation` emit, and asserts a floor of 7 so an empty scan cannot pass vacuously. |

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` · `deferred`
(issue, reason class, placement). A refutation names the disputed claim and the concrete evidence.
An original label is never edited — a revision is a new dated row with the original retained.

**PROCESS FAILURE — the tree was edited while QA round 4 was running.** Recorded as a finding
against the RUN, not against the code, because it invalidated evidence rather than shipped a defect.

QA started r4 against a verified-clean `98f58e5` and the worktree stopped being clean at 23:37:03
while the round was in flight; by the end `git diff --stat` showed 657 uncommitted insertions across
five source files and three test files. QA correctly refused to attest, and correctly identified two
would-be regressions as artifacts of importing a half-written file (`NameError: iter_component_refs`,
and an `artifact_kind` literal error) — both traced to file mtimes inside the run window, neither
reproducing afterwards. **No phantom finding was filed, and none is carried here.**

This is the THIRD instance of a hazard this repository's own memory already names, and the second
time in this slice: `WORKTREE_DIRTY` twice against `scripts/wave_gate.py` (row S10-02), and now once
against live QA. The earlier mechanism — "launch only from a verified-clean tree" — was necessary but
not sufficient, because it governs the LAUNCH and this failure happened mid-run.

**It then happened a FOURTH time, in round 5, after I had written the rule down and told QA the tree
would stay frozen.** That is the whole point of the structural-fix rule applied to process: a written
rule I had just authored did not survive its first contact with a Codex round landing mid-QA. Stating
it again would be the fourth instance patch.

**The mechanism, and it is not mine.** QA built
`.claude/agent-memory/boomi-qa-tester/harness/tree-freeze-guard.py`, which does not check git at all
— it hashes the BYTES of every loaded `boomi_mcp`/`server` module and compares PRE-import,
POST-import and at-exit. PRE vs POST proves no edit raced the import (the exact link round 5 could
not close for its `r5_04` run); POST vs EXIT proves stability during the scenario. It is
control-verified in both directions, exits `3` so a batch runner can distinguish "tree moved" from
"product failed", and it correctly does NOT fire on this codebase's pervasive lazy imports — the
module set legitimately grows mid-scenario, so the invariant is the narrower "files already read have
not changed underneath us". From round 6 on, every scenario runs inside it and every finding carries
a stamp; a moved stamp makes the finding VOID rather than failed.

That converts the failure from "the operator must remember" into "the measurement refuses to lie",
which is the only form of this fix that has held anywhere else in this slice.

**What was salvaged, and what it cost.** Round 5 bounded its runs by timestamp against the edit
window and attested three of five: the owed regression sweep (scenarios 1–7, all PASS), conflict
policy, and binding staleness. One run (`r5_04`) is recorded **VOID, not failed**, and the
adversarial probes did not run. Round 4's owed sweep is therefore DISCHARGED; what round 6 carries is
round 5's own unrun remainder plus everything the round-2 corrections changed.

### DC-3 structural fix (mandatory — three instances, all found in one review round)

**The mechanism.** A serialized document is searched as TEXT and every matching string is treated as
structural: the IR's JSON dump scanned for `$ref:` to derive dependencies (CX1-10), the same scan
deciding self-reference (CX1-06), and the emitted XML scanned for `id-<key>` to detect a leaked
placeholder (CX1-05). Each was wrong in the same direction — authored `message` text, script bodies,
static values and templates are ordinary caller content that can legitimately contain either token —
and the consequences ranged from a spurious `INTEGRATION_DEPENDENCY_REQUIRED` to refusing a correct
process after its dependencies had already been written.

**The invariant.** What counts as a reference is decided by the SCHEMA. `ComponentRefV1` is a
distinct annotated type carried by exactly the reference-bearing fields, so
`models.process_ir.iter_component_refs` walks the typed model and yields only those values. The
discriminator is the validator's own identity in the field metadata — not a `*_ref` name convention,
which would be a hand-model of the same fact and would go stale the moment a reference field is
named anything else.

| Consumer | Was | Now derived from |
| --- | --- | --- |
| dependency discovery (CX1-10) | every `$ref:`-prefixed string in `model_dump(mode="json")` | `iter_component_refs`, i.e. `ComponentRefV1`-annotated fields |
| self-reference refusal (CX1-06) | the same scan, with the root's own key silently excused | the same walk, refused explicitly at preflight |
| leaked-placeholder guard (CX1-05) | substring search of the emitted XML | exact match against a whole ATTRIBUTE VALUE, on the parsed artifact |

CX1-05 cannot use the walker — its input is XML, not the IR — so it takes the same principle in the
XML's own terms: a structural reference is the entire value of an attribute, authored prose is not.
Attribute-value equality is derived (all attributes) rather than an enumeration of attribute names,
which would be a hand-model of the emitter.

**Sibling sweep.** Every text scan over an IR or an emitted artifact in the changed set was
enumerated and converted; `_iter_envelope_refs` was already field-driven and is unchanged. The
`$ref` prefix check that remains in `_check_process_root_dependencies` now runs over values the
walker yielded, so it classifies nothing on its own.

**Non-vacuity witness.** `test_authored_text_that_looks_like_a_reference_is_not_one` authors a
`message` containing both `$ref:ghost` and `id-conn` and asserts the derived reference set is exactly
`["$ref:conn", "$ref:op"]`, then compiles the same document end to end and asserts success. Under the
old scan `ghost` was a dependency and the compile was refused.

**Coverage claim, derived from the authority's full case set.** The walker's universe is every
`ComponentRefV1`-annotated field in `models/process_ir.py` — 14 field declarations across the node
union — and it recurses through models, sequences and mappings, so a reference field added to any
node kind is covered without an edit here. Independently corroborated: QA r3 derived the same
universe from the SERVED schema (`get_schema_template("AuthoringRequestV1")` → `$defs` → fields
described as an opaque component reference) and authored each one twice, declared and undeclared,
finding zero holes.

**QA round-3 note — the acceptance criterion, and the two adversarial questions.** r3 reports the
headline capability working **unaided at the public boundary**: `plan` → `compile` → `apply`
creates a real process component, and all four of r2's instrumented-arm deviations were retired by
never being used. Scenario 3 is decisive on the update-preservation question this slice's design
turns on — desired 1831 bytes vs merged 2574, served `submitted_xml_digest` equal to the digest of
the MERGED bytes and not of the desired ones. Scenario 4 carries two firing controls (out-of-band
edit of the root alone, and deletion of the root), so its clean result is a measurement rather than
an absent check.

Both questions the correction batch was least sure of came back answered:

- **The binder premise HOLDS.** QA derived the universe from the SERVED schema rather than from a
  code read — `get_schema_template("AuthoringRequestV1")` → `$defs` → every field described as an
  opaque component reference, **10 fields**, each authored twice (declared, which must compile;
  undeclared, which must refuse). Zero holes. Six are fully non-vacuous; the other four have
  controls blocked by unrelated semantic rules, but their undeclared arm still refuses with
  `INTEGRATION_DEPENDENCY_REQUIRED`. The fail-closed post-condition never fired, so it stands as an
  untriggered backstop rather than as demonstrated coverage — recorded that way deliberately.
- **SELF-r2-01 blocks nothing legitimate.** `conflict_policy` defaults to `reuse`, so re-plan,
  re-compile and re-apply of an already-applied intent all still pass, as do the update-shaped and
  partially-existing variants. Only an explicit `conflict_policy="fail"` against an existing
  component is refused — which is what `fail` means, and apply refuses it regardless.

One arm is recorded **unverified, not passed**: `error_ambiguous_match` needs two identically-named
components, which the builder cannot produce (it reuses) and which both direct-API routes refuse
with HTTP 400 on this account. QA adds a code reading worth carrying to the review gates — with two
candidates `existing_id` is `None`, so apply takes the plain create branch, which makes that arm's
message "apply would refuse it" look inaccurate. Not acted on here: it is a pre-existing legacy
message outside this slice's changed inputs, and acting on an unverified code reading is what the
refutation discipline exists to prevent.

**Non-blocking correction batch (the ONE batch, folded into this blocking correction per
`CLAUDE.md` step 8).** Three prose claims asserting "a direct ProcessIR intent is plan/compile-only
by design" were corrected in `tests/_m12_11_support.py`, `tests/test_m12_11_apply_verify.py` and
`tests/test_m12_11_revision_binding.py`; one of them cited
`test_a_direct_process_ir_intent_cannot_be_applied`, a test this slice had already renamed out of
existence. The batch mutates the tree, so it takes the affected QA and the fix-only review with the
rest of this correction rather than riding in unvalidated. Reading those claims against the shipped
behaviour is what surfaced SELF-r2-01, which is a blocking-class defect and not prose at all.

**QA round-2 note — the root cause behind the cluster, and what was done about it.** QA's own
diagnosis was `grep -rn "_execute_canonical_process" tests/` → **0 hits**. All 71 unit tests over the
canonical and materialization modules passed by calling the pieces directly with hand-built plans,
so three deterministic first-call failures (r2-03, r2-04, and r2-02's activation) shipped green.
That is a testing-strategy defect, not five coding slips, and the correction is
`tests/test_issue_153_canonical_apply_e2e.py`: every test enters through
`build_integration_action` — the function the MCP tool layer calls — with only the network boundary
faked.

Non-vacuity of that file was measured, not asserted: against the pre-fix `src/` (stashed, tests
unchanged) **13 of 16 behavioural tests fail**. The three that pass are the deliberate controls (the
additive-record-shape check and the two "the relocatability probe is not simply refusing everything"
arms). Building that control also exposed **three tests that initially passed on the broken tree for
the wrong reason** — the r2-02 probe routed through the typed root, where `_legacy_plan_echo`
swallows the KeyError; the r2-08(a) canary probe measured 16 characters past a 15-character leak; and
the r2-08(c) probe used a typed payload that `_reject_malformed_authoring_request` decorates before
the blanket handler is reached. Each was re-aimed at the route QA actually measured. This is the
second and third time in this slice that a guard passed without exercising the breaking path, and it
is recorded here as a recurring hazard of this workstream rather than as three separate slips.

**S2-01 / S2-02 measurement note — and the vacuous control that nearly passed.** The rebaseline was
accepted only after a field-level diff, because a digest that moved for an unexplained reason is how
an unintended contract change ships. Measured:

- Exactly **6** served artifacts moved — the same six `tests/_m12_12_legacy_inventory.py --check`
  named, no more.
- `SS-SCHEMA-TEMPLATES:schema_name=IntegrationSpecV1`: `properties` `13 -> 14`, **added exactly
  `['processes']`, removed `[]`** — zero fields dropped, retyped or resized; `authority_versions`
  unchanged.
- Structural leaf diff over the whole inventory: `added=3049`, **`removed=0`**, `changed=38`, with
  every added leaf under the four schema-template artifacts that now inline ProcessIR.
- `scan_contract.python_source_count` `208 -> 209` — the one new module,
  `src/boomi_mcp/models/process_component.py`.

**The first control was VACUOUS and is recorded because it nearly produced a false "benign" claim.**
The first comparison read `schema_hash` and `revision_binding.schema_revision` off each template's
value. The `IntegrationSpecV1` template envelope carries NEITHER key — it uses `authority_versions` —
so both sides read `None`, compared equal, and the artifact reported "same" while its `sha256` had in
fact moved (`5edbb6db… -> 32c816eb…`) and its canonical length had grown 5.4x. The corrected
comparison asserts a positive control (the extraction must find >= 10 properties) before trusting any
"unchanged" verdict, which is the same failure class this repository has hit at #149, #151 and #165.

**Payload-growth observation, recorded for the review gates rather than silently accepted.** The
served `IntegrationSpecV1` template is now ~5.4x larger (11.4 KB -> 61.7 KB canonical) because a
self-contained JSON Schema must inline the referenced ProcessIR models. This follows necessarily from
issue item 4 (`IntegrationSpecV1` gains `processes`) and is not avoidable while the field is typed,
but it IS a served-contract change an MCP caller will observe, so it is surfaced here rather than
buried in a rebaseline.

**INH-DR-1 failure-mode correction (measured at step 4).** #149's row names the raw crash as a
`KeyError` at `roots[key]["body"]` in phase 2. Measured by removing the new preflight and passing a
raw dictionary, the ACTUAL exception is `AttributeError: 'dict' object has no attribute
'model_dump'`, raised earlier: `_compose_process_roots` dumps every direct root before phase 2 ever
indexes one, so the cited line is unreachable for a plain dictionary. It IS reachable for a value
that has `model_dump` but returns a body-less mapping. Same defect class either way — an untyped
crash on caller input — and the tests assert the TYPED refusal rather than any particular raw
exception, so they hold for both. Recorded because the inherited text is now known to be imprecise,
and a later reader comparing it against the code would otherwise find a mismatch with no explanation.

**INH-DR-1 trigger correction (recorded at instantiation, before the Stage-1.5 commit — this row has
never been committed, so this is instantiation, not an append-only revision).** The row was first
drafted asserting that `direct_process_roots` "becomes live in THIS slice, which is the recorded
trigger", echoing #149's trigger map. That is **false at this baseline and remains false under the
design this slice implements.** Measured, not assumed:

```
$ grep -rn "direct_process_roots" src/ server.py
src/boomi_mcp/recipes/composer.py:219:    direct_process_roots: Mapping[str, ProcessIRV1] = None,
src/boomi_mcp/recipes/composer.py:223:    direct_roots = dict(direct_process_roots or {})
```

Only the parameter's own definition — **zero callers**. `recipes/engine.py:1665` calls
`compose(attributed, descriptors)` without direct roots, and the recipe-root lifting bridge this
slice adds reads the recipe result's components plus its composed roots, not `direct_process_roots`.
The precondition #149 recorded therefore does **not** fire here. The composer hardening is still
performed in this slice (it is small, correct, and the issue puts it in scope), but it is
**preventive hardening of an uncalled public signature**, not the discharge of a live-path defect —
and it is recorded as such rather than carrying a justification the code contradicts. A deferral or
disposition whose recorded justification is contradicted by an in-slice measurement is void under
`CLAUDE.md`; this correction is made before the record is created so no void justification is ever
committed.

## Reachability-census rebaseline (r2 batch)

The #149 freeze went red on four tests after this batch. Recorded rather than silently
regenerated, because the reason matters and one part of it was NOT a rebaseline:

- **Four new census rows** — the canonical apply's own write edges
  (`_execute_canonical_process` -> `create_component` / `_apply_structured_update`, and
  `_apply_plan` -> `_execute_canonical_process`). These come from the **r1** wiring, not from
  this batch: the full non-KB suite was never re-run after `d94273e`, so the r1 correction
  landed without its census rebaseline. Noted as a gap in that batch's validation.
- **`route_reconciliation.unclassified` gained `_execute_canonical_process`** — NOT a
  rebaseline. That field is the census refusing an unclassified component-XML sink, and it
  was right to: the canonical route had no entry in `WRITE_ROUTES`. Fixed by adding
  `WRT-canonical-process-materialization` under a new classification,
  `canonical_process_materialization` — deliberately its own value rather than folded into
  `raw_process_capable` (no caller XML reaches it), `legacy_structured_process` (it consults
  no `process_kind`) or `typed_non_process` (it emits precisely a process root). Folding it
  in would have told #160 to retract the one route meant to SURVIVE the retraction.
- **Five served artifacts moved, all derived-aggregate movement.** Verified field-level
  before accepting, per the S2-01 discipline and with the positive control that discipline
  exists for: **0 properties added, 0 removed** across all five; every changed leaf is
  `capability_revision` (moved by the one new error-taxonomy code) or a digest embedding it.
  Inside `walked_surface_digest`, **19 of 384** entries moved and **all 19 are explained** —
  each either moved as a standalone artifact too, or carries `capability_revision` in its own
  value; **0 unexplained**. Control: 5 artifacts contain `capability_revision`, so the probe
  can see what it claims to look for.

`--write` rebaselined the fixture; the §11.2–§11.6 markdown tables were regenerated from the
same JSON in the same change (the two-way check requires it). Freeze suite after: **153
passed**. Tables were re-anchored on their own subsection HEADINGS rather than on document
order — three of them share a header row, so an order-based rewrite would silently replace
the wrong table if §11 were ever reordered.

## Commit boundaries (distinct from Stage-1.5 — recorded so the two are not confused)

| SHA | Kind | Tree state | Why it is NOT the Stage-1.5 boundary |
| --- | --- | --- | --- |
| `f2a25bc` … (step 9) | steps 6–9 (neutral materializer extraction, unified DAG, canonical apply + attestations, capability publish) | step-6 boundary: 9950 passed / 0 failed. Extraction verified against 40+ committed `process-component-v1` goldens, byte-identical. #149 census: **zero** row changes — the extraction added no legacy-reachability row. | Stage-1 live QA still not run; the slice is not complete. |
| `e833edf` … `06b9b40` | step-5 increments (shared preservation policy, execution profile, relocatable plan, wave-gate provider) | step-5 boundary suite: 9938 passed, 17 skipped, 1 failed → the ledger's own diagnostic scanner, fixed by allowlisting four production identifiers | Same reason as below — Stage-1 live QA still has not run. |
| `ec015d5` | work-preservation checkpoint (steps 1–4) | full non-KB suite green at the preceding tree: 9874 passed, 17 skipped, 0 failures | Stage 1.5 commits the **QA-validated** tree. Stage-1 live QA has not run — the implementation is incomplete (4 of 10 planned steps), so there is nothing coherent to exercise through the MCP boundary yet. This commit exists only so the work is not held solely in a working tree. **The Stage-2 review base remains the step-0 baseline `9f19aad5b280d58c02ef5cd840ff150d0193c1dd`**, so this commit does not move any review anchor. |

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop — 3, 6, 9, … —
## in the batch it governs, never reconstructed at close)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |
| L1 Stage-1 QA | 3 / 3 | `8d54f1a`, clean | **`CONTINUE`** | See the rationale block below. |

### L2 checkpoint at evaluation 3 — `CONTINUE`

Recorded after round 3's owed validation and before the next mutation.

**Per-tier counts and breadth.** Round 3 returned 4 findings: **1 Critical** (CX3-01, data-loss
anchor and source label P1) and **3 Standard** (P2). Breadth: 2 blocking classes, against round 1's 6.

**Trend vector — none worsening, several materially better:**

| Dimension | L2 r1 | L2 r2 | L2 r3 |
| --- | --- | --- | --- |
| Findings | 12 | 5 | **4** |
| P1 / critical-tier | 9 | 3 | **1** |
| Blocking classes touched | 6 | 4 | **2** |
| Findings the PREVIOUS round's fix introduced | — | 3 of 5 | **3 of 4** |

That last row is the one worth reading honestly. The absolute count is falling fast, but the
*proportion* introduced by the immediately-preceding correction is not — three of round 3's four are
defects in round 2's fixes, exactly as three of round 2's five were defects in round 1's. This is a
correction loop finding its own residue, and it is converging (9 P1 → 3 → 1) rather than oscillating.
The residue is also getting narrower each time: round 1 found capability-level breakage, round 3
found a missing guard clause, a missing field, a separator, and an incomplete list.

**New / resolved / recurring defect classes.** One recurrence: CX3-01 is the second instance of
CX1-02's pair (a clone decision made from a truthy value rather than from the action), and CX3-04 is
another DC-1 instance — notably one where MY OWN witness pinned an incomplete set as exact, which is
DC-1 committed inside a fix for something else. No class is being instance-patched: CX3-04's fix
derives the set from the emitting modules rather than extending the hand-list.

**Ruling out the other outcomes.** `CLOSE-CLEAN` is unavailable — one critical-tier finding is
unresolved at decision time, and L3 has never run. `DEFER-STANDARD-AND-PROCEED`/`-AND-CLOSE` require
zero critical residue in the loop; CX3-01 is data-loss tier and may not be deferred.
`ESCALATE-OPEN` needs validation unavailable, severity unresolvable, or no credible next action —
none holds: every round collected cleanly with archived evidence, both tiers derive from explicit
anchors, and the next actions are small and named.

**Named finite next correction.** The five fixes in this batch (CX3-01 through CX3-04 plus
QA-153-r6-01's surfaced limit), then round 4 scoped to that delta, QA r7, and the two gates that have
never run.

### L1 checkpoint at evaluation 3 — `CONTINUE`

Recorded AFTER the owed validation (QA r3 validated the r2 correction batch in full) and BEFORE the
next mutation, per the checkpoint rule.

**Per-tier counts and breadth.** r3 returned 3 findings. Derived tiers: **2 Critical**
(QA-153-r3-01 — secrets/security anchor; QA-153-r3-02 — mutation-accounting anchor; both are
source-labelled Medium, and both anchors make the tier critical regardless of label), **1 Standard**
(QA-153-r3-03, Low). Breadth: 3 blocking classes touched (secrets/security, mutation accounting,
machine-served contracts), against r2's 6.

**Trend vector — every dimension, none worsening, several materially better:**

| Dimension | r1 | r2 | r3 |
| --- | --- | --- | --- |
| Findings | 4 | 8 | **3** |
| Critical-tier | 4 | 7 | **2** |
| Blocking classes touched | 4 | 6 | **3** |
| Headline capability at the public boundary | BLOCKED | BLOCKED (5 sequential defects) | **WORKS, unaided** |
| Deviations QA needed to reach the code under test | — | 4 (D1–D4) | **0** |

The capability row is the decisive one: r2's instrumented arm existed because a caller could not
reach the chain at all. r3 retired all four deviations by never using them.

**New / resolved / recurring defect classes (computed from the rows, not free-typed).**
DC-2 is **resolved**: five instances, structural fix applied, and r3 produced **zero** new
instances of it. One class **recurs** — QA-153-r3-01 is the SECOND instance of r2-08's pair (a
framework's default error rendering serving caller content, authority ADR-001 §11 value-free
results): my r2 sibling sweep covered two arms and missed the third, which I had written in the
same batch. That triggers the structural-fix rule, and the next correction discharges it
structurally — **not** by patching the third arm. No class is being instance-patched.

**Ruling out the other outcomes.** `CLOSE-CLEAN` is unavailable: two critical-tier findings are
unresolved, and L2/L3 have not run at all. `DEFER-STANDARD-AND-PROCEED` / `-AND-CLOSE` are illegal
here — both require zero critical residue in the loop, and the critical rules forbid deferring a
secrets/security or mutation-accounting finding. `ESCALATE-OPEN` needs validation to be
unavailable, severity unresolvable, or no credible next action; none holds — validation ran
cleanly, both tiers derive from explicit anchors, and the next action is concrete and small.

**Named finite next correction.** One batch: (a) route the escape handler's message through
`_validation_error_message`; (b) discharge the recurring class STRUCTURALLY — a served-envelope
invariant asserted on every refusal shape, using the position-independent `input_value=` predicate
QA-153-r3-03 supplies, rather than the prefix sweep that missed it; (c) make the canonical
partial-failure envelope lift `error_code` and `step_result` and stop reporting `status: "updated"`
for a step that did not mutate. Then its affected QA and the owed L2/L3 gates.

Each rationale records: per-tier counts and breadth, new/resolved/recurring defect classes (derived
from the rows), the trend vector, explicit rule-outs of the other outcomes, and a NAMED finite next
correction. The decision comes after the owed validation and before the next mutation.

## Deferrals

Pointer-only — reason class, placement, and lineage live on the finding row and in the filed issue.
A deferral's issue body QUOTES the commit SHA of the already-committed checkpoint row it cites; the
issue body is never the first place the deferral exists. `window-exhausted` is single-use per
finding: the next appearance must be fixed, refuted, or escalated.

## Evidence index

Collected run directories are archived (byte-verified, allowlisted sidecars) under
`docs/architecture/evidence/issue-153/` with `index.jsonl` + `SHA256SUMS`, in the batch that
collects them. Run citations always use the COMPLETE run-dir name (`cdx-review.<suffix>` /
`cdx-gate-review.<suffix>`, backticked); a bare or shortened suffix is a scanner failure.

## Final-tree validation (filled at close; every roster gate current on the FINAL sha)

| Gate | Evidence (quoted output / run URL / archived round) | SHA |
| --- | --- | --- |
