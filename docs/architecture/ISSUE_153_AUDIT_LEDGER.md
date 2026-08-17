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
| DC-1 | a documented COMPLETENESS claim that the code does not enforce — a docstring asserting "every field", "excluded by construction", "fingerprints behaviour", or "non-vacuity", restated in prose beside an enumeration that is free to fall behind | the authority the claim is about: the model's own `model_fields`, the dataclass's own `fields()`, the runtime dispatch table `PROCESS_FLOW_BUILDERS`, the emitter registration table | S5-01, S5-02, S5-03, S5-04, S5-05, S5-06, S5-07, S5-09, S5-10 (9 — derived from the rows above) | **STRUCTURAL FIX applied in this batch** — see below |

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

Dispositions: `fixed` · `finding-refuted` · `severity-refuted` · `not-validated` · `deferred`
(issue, reason class, placement). A refutation names the disputed claim and the concrete evidence.
An original label is never edited — a revision is a new dated row with the original retained.

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

## Commit boundaries (distinct from Stage-1.5 — recorded so the two are not confused)

| SHA | Kind | Tree state | Why it is NOT the Stage-1.5 boundary |
| --- | --- | --- | --- |
| `ec015d5` | work-preservation checkpoint (steps 1–4) | full non-KB suite green at the preceding tree: 9874 passed, 17 skipped, 0 failures | Stage 1.5 commits the **QA-validated** tree. Stage-1 live QA has not run — the implementation is incomplete (4 of 10 planned steps), so there is nothing coherent to exercise through the MCP boundary yet. This commit exists only so the work is not held solely in a working tree. **The Stage-2 review base remains the step-0 baseline `9f19aad5b280d58c02ef5cd840ff150d0193c1dd`**, so this commit does not move any review anchor. |

## Checkpoints (a row is written IN FLIGHT at every third evaluation of each loop — 3, 6, 9, … —
## in the batch it governs, never reconstructed at close)

| Loop | Evaluation (window / cumulative) | SHA (+dirty) | Outcome | Rationale |
| --- | --- | --- | --- | --- |

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
