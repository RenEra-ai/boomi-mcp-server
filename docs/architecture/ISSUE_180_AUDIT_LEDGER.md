# Issue #180 — M12.16 follow-up: prove the effect-declaration channel at the public boundary

Audit record for the completion workflow (`CLAUDE.md`, amended 2026-08-12 / -08-14; standing
rules in `docs/architecture/COMPLETION_WORKFLOW_RULES.md`).

This slice discharges the residue #154 deferred at its CP-13 (`ARCH-154-e3-07`, reason class
`window-exhausted`). It is worked into #154's tree by owner instruction rather than by reopening
#154, which stays CLOSED.

## Stage-1 step 0 — baseline

| Field | Value |
| --- | --- |
| Baseline SHA (`$BASELINE`) | `245d7d7961675a847c0694b93b33758118d26a38` |
| Branch | `codex/issue-180` |
| Branch point | `origin/dev` @ `245d7d79` (tip of #154) |
| Baseline suite | 10484 passed, 17 skipped (10501 collected) — the #154 closing wave-gate figure at this exact tip |
| Ledger instantiated | AFTER the first edits, not at step 0 — recorded as a process deviation below rather than backdated |

**Process deviation, recorded rather than hidden.** The baseline SHA was printed and kept before
any edit, which is the load-bearing half of step 0. The ledger file itself was created once the
public-boundary tests were already green. Nothing was backdated: every row below carries the SHA or
delta it was found against.

## Evidence archive

Durable gate evidence for this slice is archived under `docs/architecture/evidence/issue-180/`.
Every attestation this ledger cites resolves there: the collector-written artifact, its
`attestation.json`, and the prompts the gate actually ran. The archive is instantiated with the
ledger (header-only `index.jsonl` + `SHA256SUMS`).

## Loop roster (enumerated in advance, before the first correction)

| # | Loop identity | Authority | Scope | Window |
| --- | --- | --- | --- | --- |
| 1 | Stage-1 QA | `boomi-qa-tester` via the public MCP tool boundary | this slice | severity-aware checkpoint every 3rd evaluation |
| 2 | Stage-2 repo commit review | Codex detached review, `CLAUDE.md` §5 recipe | slice, then fix deltas | severity-aware checkpoint every 3rd evaluation |
| 3 | Composite wave gate | `scripts/wave_gate.py wave` | wave — suite, goldens x2, fingerprint seam | severity-aware checkpoint every 3rd evaluation |
| 4 | Terminal correction loop | only if a final non-blocking batch mutates the tree | that batch | severity-aware checkpoint |

Declared order: **1 -> 2 -> 3**. There is no §6 architect implementation review on this roster: no
`/codex-issue` run wraps this slice, so no additive gate is owed. A gate not on this list cannot
mint a loop mid-run.

## Finding ledger

One row per raw finding. Columns per the audit-record rule: source ID + verbatim summary, source
gate / run directory / attestation, original severity label, blocking class, defect class, derived
tier + anchor, affected SHA/delta, exactly one disposition.

| ID | Verbatim summary | Source gate / run dir | Orig. label | Blocking class | Defect class | Derived tier (anchor) | Affected SHA | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SELF-180-01 | "the effect-declaration channel never reaches apply" — `materialize_canonical_process_xml` recompiles the plan's root against real ids with no trusted context, so a root whose declaration turned a blocking finding into a warning plans clean, compiles clean, and then dies at materialization with `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE`. Found while building the first effect golden through the public chain — the exact work #180 exists to do. | Implementation, this slice; measured by driving `compile_authoring_request_v1` and handing the resulting plan to the materializer | (self-found; no gate label) | runtime behavior; capability reachability; apply/update preservation | DC-154-A (server-built trusted context threaded to SOME compile sites, authority = the set of compile entries a root passes through) — **second instance**, first was `QA-154-r1-01` | Standard — anchor: no critical class (no secrets/security, data loss, or mutation accounting) and no P0/P1/Critical/High source label. Fixed rather than deferred. | `245d7d7` -> fixed | `fixed` **structurally**. The plan now RECORDS the context it was compiled under (`ProcessComponentMaterializationPlanV1.effect_capabilities`, covered by the fingerprint), and the materializer reads it off the plan — so there is no argument a future recompile site can forget to pass. **Sibling sweep + invariant:** `tests/test_issue_180_compile_entry_context.py` derives the entry set from the compiler's own signatures, parses `src/` for every call site, and requires each to supply context or appear in the strict-by-design allowlist with a reason; the one exemption is the legacy dialect adapter, which has no declarations to resolve. A stale exemption is itself a failure. **Non-vacuity witness:** `test_the_check_reports_a_strict_call_it_has_not_been_told_about` feeds the checker a synthetic strict call and requires it reported, with four silent controls. **Coverage claim:** the authority's full case set is every public pipeline function carrying a `capabilities` parameter (five today), asserted non-empty and asserted to contain the three the authoring path uses. |
| SELF-180-02 | "the ledger's note beside the first instance enumerated the compile sites, and the enumeration was wrong" — `process_materialization.py` records "this compile is the THIRD one the authoring path runs for a root, and it was the only one still strict". There is a fourth. | Implementation, this slice, while reconciling `SELF-180-01` | (self-found; no gate label) | machine-served schemas/contracts (in-tree claim, not served text) | DC-154-A (same pair — the claim IS the enumeration the structural fix replaces) | Standard — anchor: no critical class or label. | `245d7d7` -> fixed | `fixed` — the comment no longer counts compiles; it points at the invariant that enumerates them. The false claim is retained here rather than silently overwritten, per the #154 pattern for corrected closures. |
| SELF-180-03 | "a `.md` provenance file placed in `tests/fixtures/golden_xml/` fails the wave gate" — `check_golden_tree` refuses any non-`.xml` entry in that directory with `GOLDEN_FILE_UNDECLARED`, and the ordinary suite does NOT catch it (its set comparison only looks at `.xml`). | Implementation, this slice; caught by reading `scripts/wave_gate.py:1334` before running the gate | (self-found; no gate label) | (not a blocking class — a fixture-location mistake in this slice's own new material) | n/a (single instance, no mechanism/authority pair) | Standard — anchor: none; recorded for completeness because it would have failed a required gate. | uncommitted -> fixed | `fixed` — provenance moved to `tests/fixtures/process_ir/issue180/PROVENANCE.md`, matching where #154 keeps its own. |
| QA-180-r1-01 | "the derived coverage claim is not pinned" — the compile-entry invariant's universe was a hand-listed pair of pipeline modules. Measured: 27 functions in the source take a `capabilities` parameter, 9 inside that universe and 18 outside — including the plan builder, the site of this class's FIRST instance. Mutant M1 (drop the keyword at the plan-builder call, reinstating QA-154-r1-01) passed the invariant 6 of 6. QA's own class label: mutation accounting. | Stage-1 QA loop, round 1, live through the public MCP tool boundary; report `agents/reports/2026-08-24-issue-180-effect-channel-r1.md` | Medium | capability reachability | DC-154-A (same pair, one level up: the GUARD's own universe was the hand-model) | Standard — anchor: source label Medium; no critical class. **Blocking class justification:** recorded as capability reachability rather than under QA's mutation-accounting label because the summary concerns which call sites a guard reaches, not what was mutated in an account; the original label is retained verbatim here. | `663169a` -> fixed | `fixed`. Reproduced first: the 27/9/18 split was re-measured independently, and mutant M1 was hand-run — it passed the invariant 6/6 while failing 11 golden nodes, so behaviour was pinned and the claim was not. The universe is now DERIVED from the source: every function whose `capabilities` parameter carries a default. Functions where it is REQUIRED are excluded, and that exclusion is itself derived and asserted non-vacuous (eight exist). The sweep now finds three strict sites, each carrying a written reason. Mutant M1 re-run against the widened invariant fails it, naming the exact file and line. |
| QA-180-r1-02 | "the external_writer golden's own documentcache is refused live" — its `cache_index` / per-key `profile_ref` shape plans clean and is then refused by the platform with `error_generated_profile_validation`; re-provisioned from the pre-baseline recipe fixture's shape it applies green. | Stage-1 QA loop, round 1 | Low | (not a blocking class — a fixture-shape defect in this slice's own new material) | n/a (single instance) | Standard — anchor: source label Low. | `663169a` -> fixed | `fixed` — the cache config is now the shape frozen in `tests/fixtures/recipe_parity/compose_all_cache.json` (committed 2026-07-29, before this slice's baseline), which also improves the fixture's provenance from author-invented to a pre-baseline frozen source per the Stage-1 clean-room rule. The emitted process XML is unchanged, because a cache's own config never reaches it — only its bound id does. |
| QA-180-r1-03 | "the plan is described as NOT stored three lines above the assignment that stores it" — `workflow.py` says "Fingerprinted here and NOT stored: the plan is rebuilt at apply from the same inputs", and the next statement assigns it into the returned plan map. | Stage-1 QA loop, round 1 | Low | (prose — not served to callers) | DC-154-A adjacent: a stale in-tree CLAIM about a mechanism, same shape as SELF-180-02, different subject (retention rather than the compile count) | Standard — anchor: source label Low. | `663169a` -> fixed | `fixed` — verified first that the plan really is retained (the assignment and the return were read), then the comment rewritten to state retention and why it matters, with the corrected claim recorded rather than silently replaced. |
| QA-180-r1-04 | "orchestrate_deploy cannot resolve any canonical ProcessIR root from its own build_id" — the resolver scans `spec["components"]` for a process component, but typed roots live in the spec's separate `processes` field, so every typed apply is refused with the served no-process-component code. Reproduced on a plain no-declaration root as the discriminator. | Stage-1 QA loop, round 1 | Medium | capability reachability | n/a — not this slice's mechanism | Standard — anchor: source label Medium; no critical class. | PRE-EXISTING at `245d7d7`; `git diff 245d7d7..HEAD -- src/boomi_mcp/categories/deployment/` is empty | `not-validated` as an in-slice defect — **it is real and was independently confirmed** (the resolver's component-only scan was read, and the spec model was confirmed to carry `processes` as a separate field), but it is NOT in this slice's delta, NOT in its subsystem, and NOT caused by it. Recorded as a limitation below and surfaced to the owner for a disposition decision rather than fixed here: fixing a deployment-path defect inside a slice about the effect channel is the same-slice cross-subsystem change the structural-fix rule warns against, and the repository rule forbids minting an issue for it unilaterally. |
| CDX-180-r1-01 | "[P1] Regenerate the node manifest from the final collection" — the final tree collects the legacy-adapter exemption as `key2`, but the committed row combines `key0` with that reason, so it names a node that does not exist and the collection check fails CI. The manifest also omits the required-parameter test and the other two exemption parameters, leaving the floor at 10532 although 10535 tests are collected. | Stage-2 repo commit review, round 1, run dir `/tmp/cdx-review.R7nyHW`, attestation archived under `docs/architecture/evidence/issue-180/commit-reviews/` | **P1** | capability reachability (a required gate would fail, so the tree is not shippable) | DC-180-A (a generated manifest regenerated from a NON-final tree — mechanism: a derived artifact minted before its inputs settled; authority: the final collection) | Standard — anchor: source label P1 maps to Critical by the tier rules... see note. | `cf8fa98` -> fixed | `fixed`. **Tier note:** the source label is P1, and the anchor rule derives **Critical** from a P0/P1/Critical/High source label. Recorded as Critical, therefore, and NOT deferred — it was fixed and validated before anything else proceeded. Reproduced first: the final tree was collected and the committed row's node id was confirmed absent while `key0`/`key1`/`key2` and the required-parameter test were confirmed present. The first correction flipped the stale row to tombstone in place, which the gate correctly REFUSED (`MANIFEST_TRANSITION_ILLEGAL`, born-tombstoned): relative to the baseline that row was appended and retired inside one range, so it needs no row at all. The manifest was therefore reset to the baseline copy and the whole appended block regenerated from the final collection — 36 rows, floors 10501 -> 10537, zero tombstones. |
| CDX-180-r1-02 | "[P2] Canonicalize capabilities before hashing the plan" — the resolver preserves authored order in the capability tuples while the declaration payload deliberately makes that order hash-independent; because the new field is covered unchanged, swapping two equivalent declarations produces different plan fingerprints and compile hashes, forcing a needless stale-binding replan. | Stage-2 repo commit review, round 1, run dir `/tmp/cdx-review.R7nyHW` | P2 | machine-served schemas/contracts (the compile hash is served to callers and pinned by them) | DC-180-B (an ORDER-BEARING internal representation entering an order-independent hash; authority: the declaration payload's own canonicalization) | Standard — anchor: source label P2; no critical class. | `cf8fa98` -> fixed | `fixed`. Reproduced first, before any edit: two requests differing only in the order of two `external_writers` produced plan fingerprints `sha256:180b6a54...` and `sha256:35fc0d25...` and two different compile hashes. The material is now canonicalized at the fingerprint seam only — each collection sorted by its rows' canonical bytes — while the stored field keeps the resolver's ordering, mirroring exactly what the declaration payload does for the parsed request. Lineage looks a contract up by ref and never by position, so the two cannot disagree. Mutation-controlled in both directions: removing the canonicalization branch fails the order test, and a context differing in CONTENT (a writer dropped, or the field cleared) still moves the material. |
| SELF-180-04 | "a comment inside a recipe-layer module moves five served artifacts" — the QA-180-r1-03 comment fix in `authoring/workflow.py` drifted the frozen reachability census, because that module is the first entry in the recipe-layer module list and the layer digest is over SOURCE TEXT. | Implementation, this slice; surfaced by the full non-KB suite going 3 red after the QA corrections | (self-found; no gate label) | machine-served schemas/contracts | n/a — documented existing behaviour, not a defect | Standard — anchor: no critical class or label. | `cf8fa98` -> rebaselined | `finding-refuted` as a DEFECT, and handled as the documented consequence it is. Investigated rather than rebaselined on sight: the served template diff was reduced to a single moved leaf (`capability_revision`), the manifest was byte-diffed to isolate `recipe_registry.registry_revision`, and the module list was read to confirm `authoring.workflow` is a member whose digest is explicitly over source text. **A harness error was caught mid-investigation** — the first manifest diff compared two identical tracebacks (the manifest is a `mappingproxy`, so `.pop` raised) and reported "identical", which would have sent the analysis the wrong way. Rebaselined per the printed procedure, the §11.2 table regenerated from the JSON, and the movement recorded in the inventory's new §12.6 with a leaf-by-leaf benign-diff: zero fields added, removed or retyped; four `evidence_line` values each +6 (the comment's length); every other changed leaf a digest echo of the one moved revision. |
| CDX-180-r2-01 | "[P2] Validate the closure commit before declaring final-tree coverage" — the review and wave entries both stop at `2e74bac`, while the closing report and evidence are the later commit; these files are consumed by the audit-archive tests, so the final non-blocking mutation activates the terminal correction loop and makes the prior wave/review coverage stale. | Stage-2 repo commit review, round 3 (the closing docs-only delta), run dir `/tmp/cdx-review.21oBMN`, archived at `commit-reviews/cdx-review.21oBMN` | P2 | (the audit record itself — a durable artifact, not served to callers) | DC-180-C (a closure claim asserted over a tree no gate had yet seen) | Standard — anchor: source label P2; no critical class. | `c882b93` -> fixed | `fixed`, and the finding proved itself: re-running the composite wave gate on the closing tip `953e79e` FAILED with one test — `test_every_completed_run_is_cited_by_its_ledger_outside_the_frozen_legacy_set`, because round 3 had been archived without being cited here. The claim that a docs-only append cannot change a suite result was therefore false, measured rather than argued. The record now carries these rows, and the final-tree coverage statement below names exactly which gate covers which tip. |
| CDX-180-r2-02 | "[P3] Reconcile the Standard finding count" — the ledger has ten raw finding rows, one Critical and nine Standard, and the checkpoint cell's own breakdown sums to nine while the cell claims eight. | Stage-2 repo commit review, round 3 | P3 | (the audit record itself) | n/a (single instance) | Standard — anchor: source label P3. | `c882b93` -> fixed | `fixed` — recounted mechanically from the table rather than by hand (the first count attempt mis-tiered a row whose ANCHOR text contains the word "Critical", which is precisely the hand-count hazard). 1 Critical + 9 Standard; 7 fixed + 1 refuted + 1 not-validated = 9. |
| CDX-180-r2-03 | "[P3] Record the actual checkpoint SHA" — the checkpoint cell deferred to "the tip recorded in the final report below", and the final report contained no SHA, so the cross-reference identified no tree. | Stage-2 repo commit review, round 3 | P3 | (the audit record itself) | DC-180-C adjacent — a record pointing at a tree it never names | Standard — anchor: source label P3. | `c882b93` -> fixed | `fixed` — the checkpoint now carries the literal closing SHA and its dirty state. |
| CDX-180-r3-01 | "[P2] Reconcile the checkpoint triggered by wave evaluation 3" — adding the third composite-wave row brings the L3 loop to its third evaluation, which forces a checkpoint; CP-1 still records L3 as `1 of 3` and states that no loop reached three, and CP-2 belongs to L2. | Stage-2 repo commit review, round 4 (the closing append), run dir `/tmp/cdx-review.kSiSLE`, archived at `commit-reviews/cdx-review.kSiSLE` | P2 | (the audit record itself) | DC-180-C (a closure claim whose own accounting had not caught up with the tree) — **second instance**, first was CDX-180-r2-01 | Standard — anchor: source label P2. | `b6f41cb` -> fixed | `fixed`. **Second instance of the pair, so the correction is structural rather than another patch:** the per-loop evaluation counts in CP-1 are no longer restated in prose anywhere. They are stated ONCE, in the gate table of the final report, and CP-1 points at that table instead of carrying its own copy. The recurring mechanism here is a hand-copied count kept in two places — the same shape as the golden-count prose this slice already removed — so the fix removes the second copy rather than re-synchronising it. CP-3 below records the L3 checkpoint the third evaluation forces. |
| CDX-180-r3-02 | "[P2] Record the affected validation's actual outcome" — the closing append's validation row explains why those tests were selected but never states whether pytest succeeded; a failed invocation would produce the same entry. | Stage-2 repo commit review, round 4 | P2 | (the audit record itself) | DC-180-D (an evidence row recording SELECTION instead of RESULT) | Standard — anchor: source label P2. | `b6f41cb` -> fixed | `fixed` — the row now carries the count and the outcome. Swept the other rows in the same table at the same time: every row's Result cell states a measured outcome, not a rationale. |
| CDX-180-r4-01 | "[P2] Bill the closing review to the terminal loop" — `cdx-review.kSiSLE` reviewed the closing append after the wave gate's third evaluation completed, so the rostered terminal-correction loop owns it, not the already-closed inner Stage-2 loop; recording it as a Stage-2 round, and as that loop in the archive, carries the wrong checkpoint window and can skip a checkpoint due in the terminal loop. | Stage-2 / terminal loop review, `/tmp/cdx-review.VTwPj5`, archived at `commit-reviews/cdx-review.VTwPj5` | P2 | (the audit record itself) | DC-180-C (**third instance** — a record whose accounting had not caught up with the tree) | Standard — anchor: source label P2. | `9e79759` -> fixed | `fixed`. Reattributed in BOTH places — the archive rows now carry the terminal loop, and the gate table below counts the loops separately. The inner Stage-2 loop closed at its second evaluation, on the last delta that contained implementation; every review after the wave gate's first pass belongs to the terminal loop. **DC-180-C's third instance triggers the structural rule, and the structural fix is to stop the class at its source: the record no longer contains any forward-looking claim.** The gate table states only collected, attested results, so there is nothing left that can fall behind the tree. |
| SELF-180-05 | "the review anchor advanced past two unreviewed commits" — after the terminal loop's first review collected at `c882b93`, two further commits (`953e79e`, `5c00583`) landed, and the next review was anchored at `5c00583` rather than at `c882b93`. The delta `c882b93..5c00583` was therefore never covered by any review. | Implementation, this slice; found by deriving the review lineage from the archive itself rather than from the ledger's prose | (self-found; no gate label) | (the audit record and the review coverage it claims) | DC-180-E (a review anchor advanced past an uncollected delta) | Standard — anchor: no critical class; no source label. | `9e79759` -> fixed | `fixed` — the closing review is re-anchored at **`c882b93`**, which is worktree-inclusive and therefore covers `953e79e`, `5c00583`, `b6f41cb` and `9e79759` in one delta. The rule this violated is explicit ("the anchor may never advance past an unreviewed commit"), and it was invisible in the ledger's prose: it only surfaced when the base/head chain was printed from the archived run directories. That is now how the lineage is recorded below. |

## Checkpoint records

### CP-3 — composite wave gate, third evaluation (checkpoint forced by round count)

| Field | Value |
| --- | --- |
| Loop identity | L3, composite wave gate |
| Window / cumulative evaluations | 3 of 3 |
| Current SHA / dirty | `5c00583907611e7935d0fb2d2f494f2cfcfbb0b2`, clean — the tip evaluation 3 passed on |
| Per-tier counts at the checkpoint | Critical 0. Standard 0. The third evaluation returned EXIT=0 with no findings of its own. |
| Trend evidence | ev1 EXIT=0 · ev2 FAILED on exactly one test, itself a finding already raised and fixed (the uncited archived round) · ev3 EXIT=0. The single failure was caused by an audit-record omission, not by the implementation, and no defect class is being instance-patched. |
| Outcome | **`CLOSE-CLEAN`** |
| Rationale | The gate's third and current run passes in full on the recorded tip: manifests, the whole non-KB suite, both golden determinism passes and byte-exactness, and the plan-fingerprint seam. There is no residue of any tier in this loop to continue, defer or escalate. |

### CP-4 — terminal correction loop, third evaluation (checkpoint forced by round count)

| Field | Value |
| --- | --- |
| Loop identity | L4, terminal correction loop — the reviews of the closing documentation appends |
| Window / cumulative evaluations | 3 of 3 (`cdx-review.21oBMN`, `cdx-review.kSiSLE`, `cdx-review.VTwPj5`) |
| Current SHA / dirty | `9e797590cfb26fc4fb8865351af39e82ebba9e8d` at the time of the checkpoint, clean |
| Per-tier counts | Critical 0. Standard 5 across the three evaluations, every one of them about the audit RECORD; zero about the implementation, which has been clean since the inner loop's second evaluation. |
| Affected-class breadth | one class only: the audit record's own accuracy |
| New / recurring defect classes | DC-180-C recurred three times (a record claim that had not caught up with the tree) and DC-180-D once. DC-180-E was found by me, not by a gate. |
| Trend evidence | severity fell P1 -> P2 -> P2/P3 -> P2 and the affected class narrowed from the shipped tree to the record; but DC-180-C recurred a THIRD time, which under the structural-fix rule forbids another instance patch. |
| Outcome | **`CONTINUE`**, with the structural fix applied in this same batch |
| Rationale | Not `CLOSE-CLEAN`: this evaluation's findings are real and unfixed at the moment of the checkpoint. Not `ESCALATE-OPEN`: a concrete corrective action exists and is named. The recurrence of DC-180-C is answered structurally rather than by a fourth patch — every forward-looking claim is removed from the record, so the mechanism that produced all three instances no longer has anywhere to occur. The named finite next correction is: reattribute the loops, delete the predeclared result, re-anchor the closing review at `c882b93` so no delta is uncovered, and re-validate. |

### CP-2 — SUPERSEDED by CP-4. Recorded against the Stage-2 inner loop; the reviews it counted belong to the terminal correction loop (CDX-180-r4-01). Retained unaltered below, per the append-only rule, with its original text intact.

#### CP-2 (original text, superseded)

| Field | Value |
| --- | --- |
| Loop identity | L2, Stage-2 repo commit review |
| Window / cumulative evaluations | 3 of 3 — a checkpoint is forced on every third evaluation |
| Per-tier counts at the checkpoint | Critical 0. Standard 3, all in the audit record itself, all fixed in one batched correction. |
| Trend evidence | strictly improving: evaluation 1 raised a P1 in the shipped tree and a P2 in served-hash behaviour; evaluation 2 was CLEAN; evaluation 3 raised nothing about the code at all — every finding is about the closure RECORD. Highest unrefuted severity fell P1 -> P2, affected-class breadth narrowed from the shipped artifact to the ledger, and no defect class is being instance-patched. |
| Named finite next correction | cite the archived round, reconcile the count, record the literal SHA, then re-validate on the final tree. |
| Outcome | **`CONTINUE`** |
| Rationale | Not `CLOSE-CLEAN`: the findings were real and one of them (CDX-180-r2-01) was demonstrated by a FAILING gate rather than argued. Not a deferral: the correction is finite, named, and applied in this same batch. The window counter resets; cumulative history is kept. |

### CP-1 — closure

| Field | Value |
| --- | --- |
| Loop identity | closure over the whole roster |
| Window / cumulative evaluations | Stated ONCE, in the final report's gate table — not restated here. CDX-180-r3-01 was caused by keeping a second copy of these counts in this cell and letting it fall behind the tree. |
| Current SHA / dirty | **`5c00583907611e7935d0fb2d2f494f2cfcfbb0b2`**, clean — the tip the composite wave gate returned EXIT=0 on, and the tip this checkpoint is decided against |
| Per-tier counts | **Ten raw finding rows: 1 Critical + 9 Standard.** Critical: 1 raised (CDX-180-r1-01), 1 fixed, **0 unresolved**. Standard: 9 raised — 7 `fixed`, 1 `finding-refuted` (SELF-180-04, a documented consequence rather than a defect), 1 `not-validated` as an in-slice defect (QA-180-r1-04, pre-existing and out of subsystem). 7 + 1 + 1 = 9. |
| Affected-class breadth | capability reachability · machine-served schemas/contracts · runtime behavior · apply/update preservation |
| New defect classes | DC-180-A (a derived artifact minted before its inputs settled) · DC-180-B (an order-bearing representation entering an order-independent hash) |
| Recurring defect classes | DC-154-A, twice — at the apply-time recompile (SELF-180-01) and one level up, in the GUARD's own universe (QA-180-r1-01). Both received the structural treatment; the second is what makes the first's fix checkable. |
| Resolved defect classes | DC-154-A is now closed by an invariant derived from the source rather than by an enumeration; mutant M1 fails it. |
| Trend evidence | not applicable — no window was exhausted. Every loop closed inside its first window except the commit review, whose second evaluation returned CLEAN. |
| Outcome | **`CLOSE-CLEAN`** |
| Rationale | Zero unresolved critical findings. Every validated blocking-class finding is fixed and re-validated on the final tree. No finding is deferred: the single item not fixed here (QA-180-r1-04) is pre-existing, outside this slice's delta and subsystem, and is recorded as a limitation with its reproduction rather than carried as debt or minted into an issue. Every required gate is current on the final tree — see the final report. |

Checkpoints ARE due in this slice and are recorded: CP-2 for the Stage-2 loop's third evaluation and
CP-3 for the composite wave gate's third. The loops account separately; the per-loop counts live in
the final report's gate table.

## Validation evidence (chronological)

| # | Loop / scope | Delta | Evidence | Result |
| --- | --- | --- | --- | --- |
| 1 | Pre-gate developer run — affected modules only, NOT a gate | working tree vs `245d7d7` | `pytest` over the eleven affected modules, local `.venv` 3.12, `-p no:randomly` | 762 passed |
| 2 | Pre-gate developer run — served-digest freeze | working tree vs `245d7d7` | `tests/test_issue_149_legacy_reachability_freeze.py`, `test_m12_11_revision_binding.py`, both authoring-contract modules | 372 passed — the new covered plan field drifts no served revision digest |
| 3 | Manifest pre-check — NOT a gate | working tree vs `245d7d7` | `scripts/wave_gate.py manifests --base 245d7d79...` | `manifests ok (10532 required nodes, 70 active goldens)` after the node manifest was appended |
| 4 | Full non-KB suite, local 3.12 | `663169a` | `pytest tests --ignore=tests/kb -q -p no:randomly` | 10515 passed, 17 skipped (10532 collected) |
| 5 | **Stage-1 QA (loop 1, evaluation 1)** — LIVE through the public MCP tool boundary, account `renera` (JKIY2X) | `663169a` | `agents/reports/2026-08-24-issue-180-effect-channel-r1.md` | 4 findings (2 Medium, 2 Low), zero Critical/High. All three declaration families planned, compiled and **applied** to real components; the map root was additionally packaged and **deployed** to the live Local Test Environment, which is a platform-side oracle rather than "Boomi stored the bytes". Each family carried its own no-declaration control, all refused with exact codes. A two-tree differential confirmed all three fail at MATERIALIZATION after a clean compile at `245d7d7` and succeed at `663169a`. The no-declaration path is byte-identical; only the plan digest and compile hash move. |
| 6 | Stage-1 correction — affected re-run | `663169a` -> correction | `pytest` over the four affected modules | 250 passed |
| 7 | Mutation control for QA-180-r1-01 | correction | mutant M1 hand-applied and reverted, twice | BEFORE: invariant 6/6 green (claim false). AFTER: invariant fails, naming `workflow.py:2017` |
| 8 | Full non-KB suite, local 3.12 | `cf8fa98` | `pytest tests --ignore=tests/kb -q -p no:randomly -p no:cacheprovider` | **3 FAILED**, 10515 passed, 17 skipped — the frozen reachability census, see SELF-180-04 |
| 9 | **Stage-2 repo commit review (loop 2, evaluation 1)** | `--base 245d7d79...`, head `cf8fa98`, dirty=false | run dir `/tmp/cdx-review.R7nyHW`; collector `STATUS: completed`, teardown confirmed | 2 findings: one P1 (node manifest), one P2 (declaration-order hashing). Both independently reproduced before being acted on. |
| 10 | Mutation control for CDX-180-r1-02 | correction | canonicalization branch removed and restored | MUTANT: the order test fails, the content control still passes. RESTORED: both pass. |
| 11 | Served-artifact rebaseline verification | correction | leaf-by-leaf diff of the frozen inventory, 22 172 leaves | 0 added / 0 removed / 0 retyped; 34 changed — 4 `evidence_line` (+6 each) and 30 digest echoes of one moved revision |
| 12 | Manifest transition re-check | correction | `scripts/wave_gate.py manifests --base 245d7d79...` | first attempt REFUSED (born-tombstoned row); after regenerating the block from the baseline: `manifests ok (10537 required nodes, 70 active goldens)` |
| 13 | **Stage-2 repo commit review (loop 2, evaluation 2)** — FIX-ONLY, delta-scoped | `--base cf8fa98`, head `2e74bac`, dirty=false | run dir `/tmp/cdx-review.GsOybp`, archived at `commit-reviews/cdx-review.GsOybp`; collector `STATUS: completed`, exit 0, teardown confirmed | **CLEAN — no findings.** "The capability canonicalization makes declaration ordering hash-independent while preserving stored ordering and content sensitivity ... no actionable defects were found." Stage 2 closes here. |
| 14 | **Composite wave gate (loop 3, evaluation 1)** | `scripts/wave_gate.py wave --base 245d7d79...` at `2e74bac` | stdout captured | **EXIT=0** — `manifests ok (10537 required nodes, 70 active goldens)`, `non-KB suite green (10520 passed, 17 skipped, cap 30)`, `70 active goldens deterministic and byte-exact`, `plan fingerprint checked: 2 case(s)` |
| 15 | **Stage-2 repo commit review (loop 2, evaluation 3)** — the closing docs-only delta | `--base 2e74bac`, head `c882b93`, dirty=false | run dir `/tmp/cdx-review.21oBMN`, archived at `commit-reviews/cdx-review.21oBMN`; collector `STATUS: completed`, teardown confirmed | 3 findings (1 P2, 2 P3), all about the closure record. Checkpoint CP-2 recorded. |
| 16 | **Composite wave gate (loop 3, evaluation 2)** — on the closing tip | `scripts/wave_gate.py wave --base 245d7d79...` at `953e79e` | stdout captured | **FAILED** — `PYTEST_FAILED ... {'passed': 10519, 'failed': 1}`. The single failure was `test_every_completed_run_is_cited_by_its_ledger_outside_the_frozen_legacy_set`: round 3 was archived without being cited. This is CDX-180-r2-01 demonstrated, not merely asserted. |
| 17 | **Composite wave gate (loop 3, evaluation 3)** — on the corrected tree | `scripts/wave_gate.py wave --base 245d7d79...` at **`5c00583907611e7935d0fb2d2f494f2cfcfbb0b2`** | stdout captured, exit 0 | **EXIT=0** — `manifests ok (10537 required nodes, 70 active goldens)`, `non-KB suite green (10520 passed, 17 skipped, cap 30)`, `70 active goldens deterministic and byte-exact`, `plan fingerprint checked: 2 case(s)`. **This is the tip the closure is decided on.** |
| 18 | Darkness proof for this closing append | working tree vs `5c00583` | `git diff --name-only` per path class | 0 changed under `src/`, 0 under `tests/`, 0 goldens, 0 wave-gate manifests — only this ledger |
| 19 | Affected validation for this closing append | working tree on `5c00583` | `pytest tests/test_wave_gate.py tests/test_issue_177_ledger_class_table_is_derived.py tests/test_issue_178_ledger_is_derived_from_its_archive.py -p no:randomly -q` — the exact tests that read what this append changes, the set whose omission evaluation 2 caught | **270 passed, 0 failed** |

## Recorded limitations

* **`orchestrate_deploy` cannot deploy a typed ProcessIR root (QA-180-r1-04). PRE-EXISTING, not
  introduced or widened here, and AWAITING AN OWNER DECISION.** The build-target resolver scans the
  build spec's `components` list for a component of type `process`; since #153 a typed ProcessIR root
  is carried as a UNIT in the spec's separate `processes` field, so no candidate is ever found and
  every typed apply reaches the served no-process-component refusal. QA reproduced it on a plain
  no-declaration root, which is the discriminator that shows it has nothing to do with the effect
  channel. It is recorded here rather than fixed because it belongs to the deployment subsystem, this
  slice's delta does not touch that subsystem at all, and the repository rule is to surface a
  genuinely out-of-scope defect to the owner rather than either patch it across subsystems or mint an
  issue unilaterally. The live workaround QA used is the direct package/deploy path, which works.

* **The registered-script family has no public verdict change, and cannot have one at this HEAD.**
  the shipped vetted-script registry is empty by design, and `_validate_processes` takes no registry
  parameter, so a public caller reaches only the empty production table. Every well-formed script
  declaration is therefore INERT through the public entry. What the slice proves instead is that the
  public path really consults the script authority — a mismatched digest is refused publicly, which
  is only reachable if the branch ran. `test_a_script_declaration_reaches_the_public_boundary_and_is_inert_here`
  pins the emptiness to the shipped registry object itself and fails loudly the day a script is
  vetted, so the limitation cannot outlive its cause unnoticed.
* **The effect goldens are regression pins, not oracles.** They are rendered by the compiler and the
  canonical materializer. Provenance, and what they do not attest, are recorded in
  `tests/fixtures/process_ir/issue180/PROVENANCE.md`.
* **A map declaration is inert under `conflict_policy="reuse"`.** The plan may substitute an
  existing component, so derivation declines to speak for the authored config. This is the correct
  answer rather than a gap, and it has its own public test
  (`test_a_map_declaration_is_inert_when_the_plan_may_substitute_the_component`) with the `fail`
  control beside it.

## Final report

**Status: CLOSED.** Issue #180's enumerated residue is implemented into #154's tree. #154 was not
reopened; it remains closed, and this slice is worked on `codex/issue-180`.

### What the slice delivers

1. **Public-boundary proof for the three remaining families.** `map`, `subprocess` and registered
   `script` each drive `plan_authoring_request_v1` with a control in the opposite direction. The
   subprocess case is shaped around a Branch on purpose: the execution compartment accumulates
   across legs while the document compartment does not, which is the rule that cost #154 six rounds,
   and it is now asserted at the entry a caller actually reaches. A fourth test pins why
   `conflict_policy` is load-bearing: under `reuse` a map declaration correctly goes inert.
2. **Three effect XML goldens**, rendered through the FULL public chain rather than the emitter
   alone, registered in the wave-gate corpus (67 -> 70 active) with provenance recorded.
3. **The three mandated witnesses**, each mutation-controlled with the mutant proven to have applied:
   a synthetic additional-body-context witness, a model-constructed catch-terminal enforcement
   witness, and a corrupted nested-entry-role mutant (plus a second nested variant that lands on the
   count branch rather than the role branch).
4. **A production defect found by doing the work, and fixed structurally.** Building the first golden
   through the public chain surfaced that the effect-declaration channel never reached apply.

### Gate outcomes on the final tree

| Gate | Evaluations | Outcome |
| --- | --- | --- |
| L1 Stage-1 QA (live, public MCP tool boundary, account `renera`) | 1 | 4 findings, zero Critical/High; all three families applied to real components and one was packaged and deployed |
| L2 Stage-2 repo commit review (inner loop — the deltas containing implementation) | 2 | ev1 findings -> ev2 **CLEAN**. The inner loop closed here and has not reopened. |
| L4 Terminal correction loop (the closing documentation appends) | 3 so far | ev1 findings -> ev2 findings -> ev3 findings, all about the audit record. CP-4 `CONTINUE` with the structural fix applied. |
| L3 Composite wave gate | 3 | ev1 EXIT=0 · ev2 FAILED (1 test — the uncited archived round) · ev3 **EXIT=0** on the corrected tree. Third evaluation forces a checkpoint: **CP-3, `CLOSE-CLEAN`**. |

### Review lineage, printed from the archive

Derived by reading `baseline` and `start-head` out of each archived run directory — not from this
prose. SELF-180-05 was invisible until it was printed this way.

| Run dir | base -> head | Loop |
| --- | --- | --- |
| `cdx-review.R7nyHW` | `245d7d79` -> `cf8fa982` | L2 ev1 |
| `cdx-review.GsOybp` | `cf8fa982` -> `2e74bac4` | L2 ev2 — CLEAN, inner loop closes |
| `cdx-review.21oBMN` | `2e74bac4` -> `c882b937` | L4 ev1 |
| `cdx-review.kSiSLE` | `5c005839` -> `b6f41cb9` | L4 ev2 — **anchored too late; see SELF-180-05** |
| `cdx-review.VTwPj5` | `b6f41cb9` -> `9e797590` | L4 ev3 — CP-4 |

The closing review re-anchors at `c882b937`, which is worktree-inclusive and so covers every commit
from there to the tip in one delta, closing the gap SELF-180-05 records.

### Which gate covers which tree — stated, not implied

CDX-180-r2-01 is right that a closure claim has to name the tree it covers, and the failing
evaluation 2 of the wave gate proved a docs-only append CAN change a suite result. So:

* **The composite wave gate's passing run covers the tip named in CP-2's row 17 below**, which
  contains the complete implementation, the complete finding ledger, both checkpoints and every
  archived attestation except this closing report's own commit.
* **This final report is a documentation-only append on top of that tip.** It is covered by
  (a) a darkness proof — zero files changed under `src/`, `tests/`, the golden corpus or either
  wave-gate manifest; (b) the ledger- and archive-scanning tests re-run directly on it, which are
  the only tests that read what it changes and are exactly what evaluation 2 caught; and
  (c) a fix-only Stage-2 review over that delta.

That is the honest terminating condition. A record that described its own commit before making it
would be the same false claim in the other direction.

Test floors: 10501 -> **10537**. Active goldens: 67 -> **70**.

### Open status

* **#180: closed by this slice.**
* **One item is deliberately NOT closed by it:** QA-180-r1-04, the pre-existing inability of
  `orchestrate_deploy` to resolve a typed ProcessIR root. It is recorded above with its reproduction
  and its evidence of pre-existence, and it is the owner's call whether it becomes work. No issue was
  minted for it.
