# Issue #151 (M12.14) — audit ledger

Slice: **M12.14 — Neutral extraction, parity oracle, and reachability freeze for `flow_sequence`**
Branch: `codex/issue-151`
**Stage-1 step-0 BASELINE SHA: `47b20dd776a158c6fd096779262bbadd2867b7e3`** (== `origin/dev` at slice start)
Driver: `/codex-issue 151` (main-thread mode — this repo has no `.claude/workflows`, so the wrapping
pipeline runs `CLAUDE.md`'s own workflow rather than a composable Workflow engine).

## Loop roster (enumerated in advance, before the first correction — CLAUDE.md §"Loops, evaluations, checkpoints")

| # | Logical loop | Gate purpose / authority | Scope | Checkpoint window |
|---|---|---|---|---|
| L1 | **Stage-1 QA** | `boomi-qa-tester` through the public MCP tool boundary | this slice | fresh (3, 6, 9 …) |
| L2 | **Stage-2 repo Codex commit-review** | detached `codex-drive review --base` + `commit-review-collect.mjs` | this slice | fresh |
| L3 | **§6 architect implementation review** (additive, declared by the wrapping `/codex-issue` pipeline) | detached `codex-drive --gate review` + `gate-attest.mjs collect`, judged against `.codex/plans/issue-151.md` | this slice | fresh |
| L4 | **Composite wave gate** | full non-KB suite + active golden manifest + determinism/byte-identity + `scripts/wave_gate.py manifests` | Wave 0B slice set | fresh |

Declared order of the additive gates: L1 → L2 → L3, with L4 run as one composite evaluation before
landing. A gate not on this roster cannot mint a loop mid-run. Supporting QA/review runs bill the loop
that caused the correction.

## Plan artifacts

- Codex architect design plan: `.codex/plans/issue-151.md` (gate-attested; attestation copied to
  `.codex/plans/issue-151.attest.json`). `.codex/` is gitignored, so these are session artifacts, not
  tracked evidence.
- Claude implementation plan: `.codex/plans/issue-151.claude.md`.

**Recorded limitation (honest):** the §3 architect *plan* gate's run directory
(`/tmp/cdx-gate-architect.RTqqgF`) and its prompt directory were removed immediately after a successful
`gate-attest collect` (`ok:true`, `stopped:true`, `promptSha256 5dfdf3b6…`, `messageSha256 da4f15cf…`,
turn 1, no retry), per the `/codex-issue` §3 step 6 recipe, before the in-tree evidence-archive
convention was consulted. Consequently **no `index.jsonl` row cites that run** and none may be added
after the fact. The plan gate is a planning artifact, not a review gate, so no completion claim rests on
it. Every *review* gate run directory (L2, L3) is retained and archived under this directory before
deletion.

## Defect-class ledger (one row per RAW finding)

Columns: source ID · verbatim summary · source gate / run dir / attestation · original severity label ·
blocking class · defect class (mechanism, runtime-authority) · derived tier + anchor · affected
SHA/delta · exactly one disposition (`fixed` | `finding-refuted` | `severity-refuted` | `not-validated`
| `deferred`).

| # | Source ID | Summary | Gate / run dir | Orig. label | Blocking class | Defect class | Derived tier (anchor) | SHA/delta | Disposition |
|---|---|---|---|---|---|---|---|---|---|
| S1-01 | self/impl | `archetype_assembly.py` referenced `IntegrationComponentSpec` without importing it, so `build_from_archetype`/`import_integration_draft` returned `ARCHETYPE_BUILD_FAILED` for `api_to_api_sync`. Caught by the repo suite (67 failures) before any gate. | Stage-1 unit run, working tree | n/a (self-found) | runtime behavior | (hand-moved symbol set, module import graph) — first instance | Standard (no critical anchor; caught pre-gate, fixed in the same batch) | working tree | `fixed` |
| S1-02 | self/measure | Served revision digests moved on 5 artifacts (`SS-CAPABILITY-CATALOG:authoring_contract`, 3 `SS-SCHEMA-TEMPLATES` payloads, `SS-SCHEMA-TEMPLATES:walked_surface_digest`) because `RECIPE_LAYER_MODULES` members changed content. | `tests/test_issue_149_legacy_reachability_freeze.py` | n/a | machine-served schemas/contracts | n/a — intended rebaseline, not a defect | n/a | working tree | `fixed` (rebaselined). **Verified benign before accepting:** a field-by-field deep diff showed changes ONLY at `capability_revision`, `actual_capability_revision`, `registry_revision` and per-schema digests — zero added/removed/retyped/resized fields, and the catalog's `actions`, `archetypes`, versions and `migrated` flags are byte-identical. |

## Pre-implementation corrections carried into the implementation plan

These are dispositions of defects found in the **plan**, before any code was written; they are recorded
here because they change what gets built, not because a gate raised them.

| # | Source | Finding | Disposition |
|---|---|---|---|
| P1 | main thread (measurement) | Architect plan step 3 seeds every emitter route from `EmitterRegistration.supported_capability`, which is the literal constant `CAPABILITY_PROCESS_IR_V1` on all 18 rows (`emitter_registry.py:592-611`) — making `routes <= deletion_routes` unsatisfiable and the freeze gate a tautology (recurrence of the #149 vacuity class). | `fixed` in plan: canonical route derived by walking the closed `ProcessNodeV1` union through the real lowering. |
| P2 | main thread (measurement) | Architect's manifest rows use `"kind":"pytest"` and a key order of `id,node_id,kind,state`; `scripts/wave_gate.py:122-137,745-747` requires exactly `kind,id,node_id,state` with `kind == "test"`. | `fixed` in plan: corrected row form. |
| P3 | codex-planner | Architect's `_M12_22_DELETION_ROUTES` literal is a second hand-model of a runtime fact. | `fixed` in plan: derived from `PROCESS_FLOW_BUILDERS | migrated_dialects() | RESERVED_DIALECTS`. |
| P4 | codex-planner | Two *distinct* `_build_field_map_params` exist (one in `api_to_api_sync`, one in `api_to_database_sync`); a naive merge would silently repoint `http_listener_to_db`. | `fixed` in plan: only the `api_to_api_sync` one moves. |
| P5 | codex-planner | `tests/test_recipe_registry.py:2751-2821` pins which modules may call the recipe engine; moving an engine call into a neutral module, or removing one from `composition.py`, breaks it. | `fixed` in plan: no engine call moves; the three call sites stay. |

### L1 — Stage-1 QA, evaluation 1 (`boomi-qa-tester`, report `agents/reports/2026-08-16-issue-151-m12-14-stage1-r1.md`)

All six dispatched scope items PASSED live. Four findings raised; all four reconciled and fixed in one
batched correction. Method note recorded because it bears on how much weight the clean items carry:
the agent ran every probe against both the working tree and a clean `git archive 47b20dd7`
extraction and diffed leaf-by-leaf (49,720 probes per tree), and it graded its own harness with seeded
defects — its first two probe designs MISSED a removed `@field_validator` and a removed optional-key
guard, and only a battery that adds keys drawn from the served `parameter_schema` caught them.

| # | Source ID | Verbatim summary | Gate / evidence | Orig. label | Blocking class | Defect class (mechanism, runtime-authority) | Derived tier + anchor | SHA/delta | Disposition |
|---|---|---|---|---|---|---|---|---|---|
| Q-01 | QA-151-r1-01 | "The extraction moved 949 executed lines … out of `patterns/archetypes/api_to_api_sync.py` — a `RECIPE_LAYER_MODULES` member — into two modules that are not members." | L1 / report r1 | **High** | machine-served schemas/contracts | (source-provenance digest coverage, `build_info.source_digest` over `RECIPE_LAYER_MODULES`) — **1st instance** | **Critical** — anchor: source gate labelled it High | working tree | `fixed` |
| Q-02 | QA-151-r1-02 | "`patterns/archetypes/__init__.py` re-exports the same class … it is walked at position 3, *before* `archetypes.database_to_api_sync` … so the `__init__` re-export **is** the live registration path." | L1 / report r1 | Medium | capability reachability | (a pattern class re-exported into a walked module creating a registration path, `PatternRegistry.from_package`) — **2nd instance** (1st was `composition.py`, fixed earlier in this slice) | Standard — no critical anchor; latent, no behaviour change today | working tree | `fixed` — **structural fix**, see below |
| Q-03 | QA-151-r1-03 | "`archetype_assembly.py:297` uses `Set[str]` but the module imports only `Any, Dict, List, Optional, Protocol`." | L1 / report r1 | Low | — (not in a blocking class; latent, provably unreachable at runtime) | (hand-copied import list vs the moved code's needs, the module's own name bindings) | Standard-equivalent; batched | working tree | `fixed` |
| Q-04 | QA-151-r1-04 | "25 new F401 dead imports (1 → 26 in `patterns/`), 15 of them in `api_to_api_sync.py`." | L1 / report r1 | Low | — (non-blocking residue) | same pair as Q-03 | non-blocking; ONE batched pass | working tree | `fixed` |

**Verification before fixing (per `receiving-code-review`).** Q-01 was re-measured independently: my
first probe called a non-existent module-level `registry_revision()` and returned empty for BOTH arms
— an inert harness that would have "confirmed" the finding for the wrong reason. Rebuilt against
`build_info.source_digest(RECIPE_LAYER_MODULES)` with a positive control (an edit inside a listed
module DOES move the digest), the gap reproduced. Q-02's walk-order claim was reproduced by
instrumenting the real `walk_packages` loop.

**Structural fix for Q-02 (second instance of the pair → mandatory in this batch).** Replaced the
enumeration ("do not re-export a pattern class into a walked module", which would have to be audited
at every module forever) with an invariant derived from the runtime authority: `from_package` now
registers a class only from the module whose `__module__` matches. Evidence:

- *Sibling sweep* — the instrumented walk showed the mechanism was not two-site but near-universal:
  **30 of 31** patterns were registering from a re-export rather than their definer, including
  `db_extract` from `archetypes/database_to_api_sync.py`, a module #160 DELETES.
- *Non-vacuity witness* — `test_from_package_registers_a_pattern_only_from_its_defining_module`
  constructs a package whose `__init__`-alphabetical sibling re-exports the class and asserts the
  registration comes from the DEFINER. It asserts which module registered it, not the count: a
  count-only assertion passes in both worlds and would not have pinned the fix.
- *Coverage claim* — derived from the authority's full case set: measured catalog-neutral, the same
  31 patterns before and after, with a `>= 31` fail-closed floor in
  `test_the_production_catalog_registers_every_pattern_from_its_definer`.

**Q-01 fix, and why it needed a rule rather than a literal.** Adding the two neutral modules to
`RECIPE_LAYER_MODULES` first broke
`test_the_layer_module_list_covers_the_whole_recipe_package`, which requires every listed module
outside the recipes package to be CLAIMED BY A RULE (clause 2 contract modules, clause 3 engine
invokers) rather than hard-coded — the guard exists precisely so the list cannot grow by taste. The
fix therefore adds a named **clause 4** (the neutral parameter/assembly layer) bounded below by an
asserted import relation: a module qualifies only if a clause-3 invoker actually imports it, so the
clause cannot drift into a package hash and
`test_the_downstream_compiler_is_not_in_the_layer_digest` still holds. Coverage restoration was then
re-measured with the original probe: mutating either neutral module now moves `source_digest`.

### L1 — Stage-1 QA, evaluation 2 (fix-delta re-run, report `agents/reports/2026-08-17-issue-151-m12-14-stage1-r2.md`)

A BROAD re-run rather than a single scenario, because the Q-02 structural fix touches
`PatternRegistry.from_package` — discovery/dispatch — which CLAUDE.md Stage 1 step 3 names as
warranting one. All four r1 findings confirmed fixed. The discovery invariant was attacked and held:
catalog identical name-for-name (31 patterns / 6 archetypes / 25 primitives); zero pattern classes
would be dropped under any walked module; fails closed on ImportError/SyntaxError/raise-at-import;
stable across import orderings, by-string vs by-module argument, and a pattern defined in a package
`__init__.py`. The `src.`-prefixed spelling trap does NOT bite — the cross-spelling call returns 0 on
BOTH trees, and the rejection buckets attribute it to the pre-existing `issubclass` check
(`not_pattern_subclass: 84`, `module_mismatch: 0`), not to the new filter. Served surface unchanged
(49,720 A/B probes per tree; the 99 differing leaves are all provenance digests; both frozen
2026-07-11 M8 compose oracles still reproduce exactly).

| # | Source ID | Verbatim summary | Gate / evidence | Orig. label | Blocking class | Defect class (mechanism, runtime-authority) | Derived tier + anchor | SHA/delta | Disposition |
|---|---|---|---|---|---|---|---|---|---|
| Q-05 | QA-151-r2-01 | "Neither new registry test pins the invariant. They pass with the filter **reverted** *and* with it **inverted**." | L1 eval 2 | Medium | capability reachability (the structural fix's mandatory non-vacuity witness) | (a guard asserting a property that cannot vary with the thing it guards, `PatternRegistry.from_package`) — 1st instance | Standard — no critical anchor | fix delta | `fixed` |
| Q-06 | QA-151-r2-02 | "Clause 4's import-relation bound is **not** load-bearing … it admits 37 internal modules — including 5 of the 7 that `test_the_downstream_compiler_is_not_in_the_layer_digest` forbids." | L1 eval 2 | Low | — (non-blocking: a comment crediting the wrong mechanism; the tree was correct) | (prose asserting a guarantee the code does not provide, the guard's own predicate) | non-blocking | fix delta | `fixed` |

**Q-05 — my witness was vacuous, and the finding is exactly right.** Reproduced before fixing: with the
`__module__` filter deleted from `registry.py`, all 17 nodes in `tests/patterns/test_registry.py` still
passed. Root cause as diagnosed: `cls.__module__` is a class attribute recording where the class was
DEFINED; a re-export binds the same object, so it reads `definer` in every world and cannot observe the
registering module — the only thing the filter changes. Replaced with the discriminating case: reload
the definer so the re-exporter keeps a STALE class object while the definer holds a new one; the two
have different `id`s, so the `seen` set cannot collapse them. Without the filter both register and the
second collides (`DUPLICATE_PATTERN_NAME`); with it, the stale binding is skipped. **Verified to
discriminate in both directions** — passes with the filter, fails with `PatternRegistryError` without
it — and the test itself asserts `stale is not live` first, so it cannot go inert if reload semantics
ever change. This is also a real production fault the filter removes, not a contrived one.

**Q-06 — verified and fixed by tightening the bound, not only the prose.** Measured the weak form: 37
internal modules admitted, 5 of them on the forbidden list. Clause 4's bound now additionally requires
membership in `boomi_mcp.patterns.*` (excluding `patterns.archetypes.*`, which clause 3 owns), which
admits 11 and **none** of the forbidden 7 — asserted directly, so the exclusion is load-bearing rather
than described. The comment now states plainly that the bound is a NECESSARY condition, not a
characterization, and that the explicit pair plus the downstream-compiler boundary pin are what fix
membership.

### L1 — Stage-1 QA, evaluation 2 addendum (procedural)

| # | Source ID | Verbatim summary | Gate / evidence | Orig. label | Blocking class | Defect class | Derived tier + anchor | SHA/delta | Disposition |
|---|---|---|---|---|---|---|---|---|---|
| Q-07 | QA-151-r2-03 | "The tree was edited during **both** of my full-suite runs." Run 1 void (three files moved mid-run, spurious `inspect.getsource` failure); run 2 green (9813 passed) but `test_issue_151_reachability_freeze.py` gained 112 lines 5 s before it ended and `test_m11_composed_examples.py` moved after, so neither file is covered by it. | L1 eval 2 | procedural | — (evidence completeness, not behaviour) | (validation evidence collected over a moving tree, the suite's own collection-time import) | Standard | fix delta | `fixed` — one full-suite run on a frozen tree, recorded below |

Valid, and it repeats a lesson this repo already recorded (#172: never edit the tree while the gate
runs). `src/` and `server.py` were frozen across both of QA's runs, so this is an evidence gap rather
than a behavioural risk — but a suite run that did not import the final bytes of two test files is not
evidence for those files. The counts reconcile exactly (QA's 9813 + 4 nodes it deselects = my 9817;
both collect the same set). Discharged by the frozen-tree run recorded in the closing checkpoint.

### L3 — §6 architect implementation review, evaluation 1

Run directory `/tmp/cdx-gate-review.D5m5ga`, gate `review`, `ok:true`, `stopped:true`, turn kind
`turn`, turnToken 1, `promptSha256 f1015c72…`, `messageSha256 385e8c6e…`, plan bytes verified present
in the prompt by the collector. `parsedVerdict: ISSUES FOUND`. The reviewer explicitly judged all four
recorded plan deviations JUSTIFIED (the rejected `supported_capability` seeding, the narrowed
extraction set, the redefined `DELETION_ROUTES`, and both live-QA additions) and confirmed the manifest
arithmetic.

| # | Source ID | Verbatim summary | Gate / run dir | Orig. label | Blocking class | Defect class | Derived tier + anchor | SHA/delta | Disposition |
|---|---|---|---|---|---|---|---|---|---|
| R-01 | §6 P1 #1 | "The deletion gate does not actually freeze each of the 14 `flow_sequence` kinds. It only checks `len(_FLOW_SEQUENCE_ALLOWED_KINDS) >= 14` and aggregates emitter keys across every specimen … A newly allowed kind with no fixture would silently pass." | L3 / cdx-gate-review.D5m5ga | **P1** | capability reachability | (an aggregate assertion standing in for a per-item one, the emitter registry + lowering) — 1st instance | **Critical** — anchor: reviewer labelled it P1 | fix delta | `fixed` |
| R-02 | §6 P1 #2 | "None of the four shipped examples is tested through dry-run apply … The required dry-run result, planned/applied action equality, and no-client-mutation assertions were silently omitted." | L3 / cdx-gate-review.D5m5ga | **P1** | apply/update preservation | (a planned acceptance test dropped on a mistaken premise, the issue's own acceptance criteria) — 1st instance | **Critical** — anchor: reviewer labelled it P1 | fix delta | `fixed` |
| R-03 | §6 P2 #1 | "The architecture inventory contradicts the final implementation … Both statements are false after the live-QA fixes, making the #160 handoff misleading." | L3 / cdx-gate-review.D5m5ga | P2 | — (handoff record accuracy; not served to callers) | (a stale authority statement outliving the change it described, the code it describes) — 2nd instance in this slice (1st was clause 4's comment, Q-06) | Standard | fix delta | `fixed` |
| R-04 | §6 P2 #2 | "`archetype_parameters.py` has no explicit `__all__`, despite the plan requiring a declared reusable export surface … `_ApiParametersLike` omits `naming`, although `_build_source_response_profile` reads `parameters.naming`." | L3 / cdx-gate-review.D5m5ga | P2 | machine-served schemas/contracts (the neutral layer's declared export surface) | (an export/shape declaration hand-written from memory rather than from the consumed set, the modules' actual importers) | Standard | fix delta | `fixed` |

**R-01 — verified, and it is the same blind spot I criticised in the architect plan, one level down.**
My own comment claimed "flow_sequence authored-kind coverage" above an assertion that only floored a
runtime constant. Fixed by attributing every authored step to the plan node carrying its
`source_path`, then asserting (a) authored specimen kinds == `_FLOW_SEQUENCE_ALLOWED_KINDS` EXACTLY in
both directions, (b) every authored step produced an emitter node (only the `sequence` body container
is exempt), and (c) per-kind attribution covers >= 14 kinds with no routeless kind. **Mutation control
run:** injecting a phantom allowed kind with no specimen leaves the OLD floors passing
(`len(allowed)=15 >= 14`) while the NEW assertion fires and names the phantom.

**R-02 — verified; my implementation plan dropped it on a factually wrong premise.** The plan's
deviation 7 said dry-run apply was "not an acceptance criterion of #151"; the criterion says the four
examples must "plan **and** apply unchanged through the existing entry". Two nodes added covering all
four shipped examples (plus the third M11 example): assert `dry_run` is true, `_success`, step-key and
planned-action equality between plan and apply, no `validation_error`, and no create/update/delete
reached the client.

**R-03 — verified.** §12.5 claimed `RECIPE_LAYER_MODULES` "was not edited"; §12.1 described
`from_package` as having no `__module__` filter. Both were true when written and false after the
live-QA fixes. Corrected in place, with the reason for the change recorded rather than the claim
silently swapped. Second instance of the stale-statement class in this slice, so both were swept: a
grep for the two falsified claims across the inventory and the ledger now returns nothing.

**R-04 — verified.** `__all__` was absent (count 0); `_ApiParametersLike` declared
`source`/`transform`/`target` while the moved helpers also read `parameters.naming` (`:572`). `__all__`
now declares the 30 reusable model names, derived by measuring what the tree actually imports from the
module rather than hand-listing, and deliberately excludes the underscore-prefixed private validators.
`naming` added to the protocol. A latent docstring corruption from the original bulk forward-reference
rewrite (which had turned the protocol's own explanation circular) was corrected in the same pass.

## Observations recorded, not fixed (out of scope for this slice)

| # | Observation | Why not fixed here |
|---|---|---|
| O1 | **Split dialect vocabulary.** The legacy-adapter registry keys on the full dialect `"database_to_api_sync/flow_sequence"` (`legacy_adapters/registry.py:36`) while the semantic-validation policy registry — and the production call site at `process_flow_builder.py:4448` — key on the LEAF `"flow_sequence"`. `lookup_policy("database_to_api_sync/flow_sequence")` therefore returns `None` (strict), and three committed `flow_sequence` specimens fail strict validation. Production is correct today only because it passes the leaf spelling. | Both files are pinned byte-identical by acceptance criterion 2 (the parity oracle must gain no capability), so this slice may not touch them. The freeze test does not hand-model the mapping: it derives the leaf with `dialect.rsplit("/", 1)[-1]` and **asserts** `set(registered_adapters()) == {leaf(d) for d in migrated_dialects()}`, so any further drift between the two vocabularies now fails a gate instead of silently selecting the strict path. |
| O2 | **Inventory `#151` ownership rows point elsewhere.** §11.5 assigns 15 ledger rows to `#151` with disposition "re-home onto the neutral extraction", but they are all compiler-side (`legacy_adapters/authority.py`, `sync_pipeline.py`, `wrapper_subprocess.py`, `semantic_validation/legacy_bridge.py`) — not the `patterns/archetypes` parameter layer this issue actually extracts. | Ownership is assigned by #149's path-scoped rules over the census, not by hand. Re-deriving them would drift a served artifact for no acceptance-criterion benefit. Recorded here so #160 is not misled into expecting those four compiler modules to have moved. |

## Checkpoint records

**L1 (Stage-1 QA) — 2 evaluations spent, no checkpoint due.** Checkpoints fall on every third
evaluation (3, 6, 9 …). Evaluation 1 = the initial scoped live pass (4 findings, all fixed).
Evaluation 2 = the fix-delta broad re-run (2 findings, both fixed). The S1 rows above are
implementation and unit-suite work, not gate evaluations, and do not count. The correction applied
after evaluation 2 is tests-only; its owed validation is a darkness proof plus the guard's own
two-directional discrimination check, both recorded below — not a third live evaluation.

**Validation owed and discharged for the post-evaluation-2 correction (Q-05, Q-06):**

- *Darkness proof.* `source_digest(RECIPE_LAYER_MODULES)` is
  `source-sha256:621dac62186d4acb29b5537f17f85d830a43da91a0c4e74026ea53a37bfd7be4` — byte-identical to
  the value QA measured on the tree it validated in evaluation 2, so `src/` is unchanged; the
  correction touched only `tests/patterns/test_registry.py` and `tests/test_recipe_registry.py`.
- *Guard discrimination.* The replaced witness was run against both a filtered and an unfiltered
  `registry.py`: passes with the invariant, fails `PatternRegistryError` without it.
- *Golden byte identity.* `tests/fixtures/golden_xml` and `tests/fixtures/process_ir/emitter_parity`
  are byte-identical to `47b20dd7`; the parity oracle and the two IR authorities are untouched.
- *Full non-KB suite.* 9817 passed, 17 skipped, 0 failed.

L2 (Stage-2 repo Codex review), L3 (§6 architect review) and L4 (composite wave gate) have spent zero
evaluations at the time of the Stage-1.5 commit.
