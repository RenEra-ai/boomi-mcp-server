I’ll review plan conformance, starting with the seven original findings and the ledger’s rationale, then trace the remaining requirements to code and evidence.
The checkout is clean and matches the requested HEAD. I’m applying the receiving-code-review skill’s verification discipline and splitting the review across governance, coverage, and historical evidence.
The ledger marks all seven original findings fixed, but closure needs more than the disposition column. I’m checking selected-artifact handling, late apply validation, and the retirement proof axes in code. The final wave-gate evidence table is still empty.
Several corrections are substantive: identity conflicts now share a check, the six-route sentinel exercises real entry points, and retirement records include ordered create traces. Targeted probes are also finding cases where a nonempty candidate profile is still trusted before the selected profile is consulted. Those cases affect watermark validation and flow summaries.
The focused checks are green so far, and all 12,005 collected test nodes match the manifest with existing IDs preserved. Separate probes still show plan gaps: compile can fall back to candidate fields when selected-profile evidence is unavailable, and apply can reach its first write when that evidence disappears during the final refresh.
The review currently supports closing three original findings: the identity conflict, competing `flows` locations, and validator consolidation with its route proof. Four remain partial: selected-artifact coverage, flow fidelity, watermark validation, and retirement fingerprint evidence. The replacement sentinel failed as expected when the actual `run_recipes` validation call was removed in memory.
The implementation substantially realizes the plan, but **four original findings remain partially closed: ARCH-157-e1-01, -02, -04 and -06.** Findings -03, -05 and -07 are closed on this tree.

I reviewed the requested baseline-to-`6e217b09249ae336a943b22a4d8e8b2dd424384e` delta and the ledger’s P1–P9, D1–D19 and finding dispositions. The repository remains unchanged.

| Original finding | Status | What the code establishes |
|---|---|---|
| **ARCH-157-e1-01 — selected-artifact coverage** | **Partial** | Conflict policy reaches resolution; direct coverage uses the shared reuse decision; successful apply-time index refreshes receive required-leaf validation. Unavailable evidence and recipe reused-map handling still violate the guarantee. See [direct exemption](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:1856) and [refresh validation](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:9621). |
| **ARCH-157-e1-02 — derived-flow fidelity** | **Partial** | Selected indexes now reach the projection, live index entries fit the served schema, and served profile names survive normalization. Candidate subshapes and reused-map mappings still enter output; comparison still discards names. See [projection](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/derived_flows.py:249). |
| **ARCH-157-e1-03 — conflicting connection identities** | **Closed** | Both create and reuse branches call `_refuse_contradictory_identity`; the original conflicting-ID and conflicting-name cases are rejected. See [shared identity check](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/governance.py:654). |
| **ARCH-157-e1-04 — selected watermark evidence** | **Partial** | Deferred watermark checks reach selected evidence, with source-field and query-consistency rules separated. Nonempty candidate indexes can still decide prematurely, and unresolved deferred checks can survive compilation. See [online recheck](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:1932). |
| **ARCH-157-e1-05 — competing flows locations** | **Closed** | Intent/recipe-input checks and the outer request guard now provide the named refusal at the original competing locations, including `config.flows` and `authoring_request.flows`. See [outer guard](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:11482). |
| **ARCH-157-e1-06 — retirement proofs** | **Partial** | The harness now hashes the actual ordered create-call documents, closing the mutation-order omission. Fingerprint evidence—or an evidenced applicability disposition—is still absent. See [measurement harness](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_retirements.py:178). |
| **ARCH-157-e1-07 — consolidation and route proof** | **Closed** | Advisory review now calls the shared destination interpreter. The replacement sentinel exercises all six real entries and detects actual invocation removal. See [advisory calls](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/transformation_review.py:695) and [sentinel](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_157_required_target_coverage.py:521). |

The remaining conformance failures are concrete:

1. **Selected-profile evidence is still lost or bypassed in three cases.**

   A failed selected-profile discovery is omitted from the result dictionary. The downstream resolver then falls back to candidate configuration unless `reference_only` is explicitly present. In a reproduced name-collision reuse case, the target was known to be reused, discovery returned no index, and **compile succeeded using candidate fields**. The required outcome is `MAP_PROFILE_INDEX_UNAVAILABLE`. See [discovery omission](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:5598) and [fallback](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/builders/transform_map_validation.py:80).

   At final apply refresh, a reused `$ref` profile qualifies for revalidation only if discovery successfully populated that dictionary. Making discovery succeed during compilation and disappear specifically during refresh caused the mocked mutation dispatcher to receive the first component. The required pre-write refusal did not occur. See [refresh condition](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:9591).

   Finally, the recipe engine validates every structured map without the direct route’s reuse exemption. The same reused map produced no coverage finding through direct authoring but was rejected through both `run_recipes` and recipe intent because its discarded candidate mappings were incomplete. See [recipe gate](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/engine.py:1737).

   **D4 explicitly requires selected-artifact authority. D3’s legacy boundary does not justify these canonical failures.**

2. **Derived flows still mix selected and discarded content.**

   `_selected_or_generated` copies the candidate generation summary and replaces its index and mappable paths. A reproduced successful plan served `Root/platform_only` in the selected field index while omitting it from the same summary’s `profile_config.root.children`. An authoritative empty selected index also falls through to candidate generation.

   Separately, the projection reads reused maps’ candidate configurations. Changing a reused map’s candidate destination to `Root/discarded_candidate_mapping` produced a successful plan that served that destination in `flows[].operations`. See [map projection](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/derived_flows.py:440).

   The ledger’s “shape from the generator, fields from the account” explanation does not justify contradictory subshapes. Item 7 requires fidelity across the generated-profile structure, indexes, identities and mappings.

3. **Watermark validation still decides from the wrong evidence—or never obtains a decision.**

   For a source profile selected for reuse by name:

   - Candidate contains `updated_at`; selected profile does not: planning reports **zero errors**.
   - Candidate lacks `updated_at`; selected profile contains it: normalization rejects **before selected-index discovery**.

   The nonempty candidate index prevents deferral, and the online pass revisits only deferred declarations. See [candidate decision](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/governance.py:1017).

   When a reference-only source’s index remains unavailable, the validator returns `False`; its online caller ignores that return. A reproduced request **compiled with zero errors and one unresolved deferred watermark**. This contradicts item 10 and D4. The offline witness is [available here](/private/tmp/issue157_e2_watermark_conformance.py).

4. **Persistent accounting does not yet enforce all promised content and retirement guarantees.**

   Comparison normalization still removes `component_name` unconditionally. Replacing a real canonical profile name with `UNRELATED WRONG PROFILE NAME` was accepted by the discharge oracle. R3 explains a representation difference between producers, but these comparisons are against each producer/case’s own baseline. Blanket omission does not establish name fidelity. See [comparison normalization](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/derived_flows.py:110).

   Retirement discharge checks only whether an ID exists. A frozen send row was accepted as retired using an unrelated JDBC record, **RET-157-05 classified RETAIN**, or **RET-157-10 classified SPLIT**. Classification, proof and applicability are not checked. See [flow discharge](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_flows_accounting.py:658).

   No committed rows currently use retirement discharge, so this does not invalidate today’s nine discharges. It leaves the explicitly requested reusable retirement mechanism incomplete. P5’s pending state is justified; these omissions are not.

5. **Retirement fingerprint evidence remains incomplete, with an applicability distinction.**

   The records compare emitted XML, ordered create-document traces, stripped plan results and plain spec digests. They do not measure execution/materialization fingerprint authorities.

   These historical legacy process specifications do not produce canonical process-root artifacts at the baseline. Therefore, the appropriate evidence may be **proven non-applicability**, plus separately measured typed-authoring hash churn. It should not invent invariant legacy artifact fingerprints. Neither the records nor a justified plan correction currently establishes that disposition.

   The served assertion that retired metadata never affected “a fingerprint” also exceeds the acknowledged full-echo hash behavior. See [served claim](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/contract.py:750).

Coverage of every numbered plan item:

| Item | Status | Assessment |
|---|---|---|
| **1. Audit and historical evidence** | **Partial** | Literal baseline, advance roster, isolated extraction, seven-producer census, ordered payloads and digest guards exist. I did not find the requested explicit consolidation of shipped #153–#156 contracts and final #153 handoff. |
| **2. Authored defaults and envelopes** | **Implemented** | Strict resolved envelope, explicit action, name/prefix defaults, structural validation, collision checks, recipe envelopes and equivalent-default fingerprints exist. |
| **3. Dependency ownership and fan-out** | **Implemented otherwise** | Ownership follows references/dependencies, including extensions, stops at roots, and preserves explicit governance. D8/D19’s transport injection is justified. The trading-partner name route remains a declared exception to folder-ID submission. |
| **4. Secrets and connection contract** | **Implemented otherwise** | Shared scanner, mutable-model recheck, materialization-mode binding policy, shared REST policy and strict header merger exist. D1/D2 and P1 explain reasonable scope and diagnostic adaptations. |
| **5. Sole required-target validator** | **Implemented** | The legacy walker is removed; all six legacy consumers, composition comparison and advisory review use the shared implementation. JSON/DB authorities, sorted gaps and empty-index deferral remain. |
| **6. Routes and selected artifacts** | **Partial** | All six entries reach coverage. The unavailable-evidence, final-refresh and recipe-reuse cases above remain. |
| **7. Typed derived flows** | **Partial** | Typed canonical preview, named output-only rejection and executable-hash exclusion work. Selected-content fidelity remains incomplete. |
| **8. Persistent accounting** | **Partial; justified structural deviation** | P5’s pending state resolves the plan’s sequencing conflict. The corpus preserves 83 flow rows and 58 endpoint/provenance rows: nine discharged, zero retired, 132 pending for #159. Canonical replay and generative tests exist; name and retirement-authority checks remain incomplete. |
| **9. Recorded-not-wired channel** | **Implemented** | Fifth typed contribution, identity/ownership validation, determinism, served results and persisted provenance exist outside executable hashes and constraints. D6’s omission from unused legacy responses is reasonable. |
| **10. Watermark/scheduling and retirements** | **Partial** | Typed declarations, persisted-DPP separation, obsolete-spelling refusals, eight RETIRE/one RETAIN/one SPLIT records exist. Selected-watermark enforcement and fingerprint evidence remain incomplete. |
| **11. Served contracts and artifacts** | **Partial** | Registries, schemas, taxonomy and generated artifacts are updated; P1–P3’s adjustments are justified. Some served guarantees remain broader than implementation/evidence. ProcessIR schema, XML goldens, golden manifest, dependencies and CI workflows are unchanged. |
| **12. Test matrix** | **Partial** | Broad coverage and the replacement six-route/removal proof exist. The counterexamples above expose missing selected-evidence, projection and accounting cases. Connection tests were reasonably consolidated into other test files. |
| **13. Public-boundary live QA** | **Implemented historically** | The reports cover both intents, supporting placement, extensions, preservation, multi-root controls, attestations, verification and cleanup. I performed no additional account mutations. |
| **14. Completion and scope boundaries** | **Partial / pending evidence** | Eight commit-review archives have matching reviewed-SHA and teardown evidence. I found no completed final composite wave gate with `--require-plan-fingerprint` or full Python 3.11 result. R12 explicitly tested nothing after `0bc76b38`; I found no later live correction-validation record. The #159/#160/#155 boundaries otherwise remain intact. |

**Governance remains on typed per-root envelopes, never on `ProcessIRV1`.** The IR model and committed schema are unchanged.

**Flows are derived and output-only, and stay outside execution authority.** That boundary is implemented; fidelity to the selected artifacts is the remaining problem.

The replacement sentinel deserves acceptance: an in-memory mutation deleting the actual `run_recipes` validation invocation made it fail specifically for `recipe_engine` and `recipe_intent`, while the other four routes remained effective.

All focused test runs passed. The legacy inventory check reported **no drift**, and **all 12,005 collected nodes matched the active manifest**, with no existing IDs removed or repointed. These checks used local Python 3.12 and do not substitute for the outstanding full Python 3.11/wave evidence.

VERDICT: ISSUES FOUND

