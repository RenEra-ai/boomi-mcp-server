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
