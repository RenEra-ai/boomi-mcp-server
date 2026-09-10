I’ll check the prior findings against the code, then assess each plan item and its evidence. I’ll keep this review read-only and separate blocking gaps from documented deviations and validation still owed.
The checkout is clean and matches the requested HEAD. I’m splitting the review across governance, selected-artifact validation and flows, and historical accounting so each gets an independent check. I’m also applying the receiving-code-review skill when reconciling the earlier findings.
I found a possible residue of evaluation 2’s retirement finding: the workflow contract now limits its fingerprint claim, but the error taxonomy still says retired metadata never affected a fingerprint. I’m checking which of that text reaches callers and whether the proof records support it.
The schema artifacts match their runtime generators exactly. ProcessIRV1, its schema, and the XML golden manifest are unchanged from the baseline. The historical-accounting check has found a more substantive concern: two new checks appear disconnected from the real discharge path, so the new tests may not establish that evaluation 2’s finding is closed.
Several gaps now reproduce on the current tree: a literal watermark profile can remain unresolved through compile, derived DB flow output can combine selected fields with candidate-required flags, and real accounting discharge calls still accept a wrong profile name or an unrelated retirement record. I’m also checking whether the plan’s runtime secret-scanning requirement was implemented at the raw `integration_spec` entry.
The 467 focused tests pass, and the legacy inventory check reports no drift. The reproduced gaps therefore remain outside what those tests establish. The raw `integration_spec` route also echoes the synthetic runtime-secret canary; I’m treating that as an unimplemented part of the plan’s security requirement, with Critical severity under the repository rules.
**The implementation substantially realizes the plan, but six blocking conformance findings remain: one Critical and five Standard.** Several earlier findings are only partially closed despite their ledger dispositions.

I reviewed baseline `ba1be9f34317d720d3b012484622ab7e2742143b` through `362414d3a8575f0adb7a66ae9c6845e84a4d63dd`. The checkout remains unchanged. The counterexamples below were reproduced locally with account boundaries stubbed; no external mutations were performed.

1. **ARCH-157-e3-01 — Critical, secrets/security: runtime scanning misses the raw `integration_spec` entry.**

   Plan item 4 requires scanning `spec.runtime` before previews, errors or result echoes. The implementation scans it only inside the typed `integration_spec` intent at [workflow.py:444](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:444). The raw public route enters [_build_plan](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:6342), which scans pipeline secrets but proceeds without scanning runtime.

   Reproduced with a valid component and:

   ```python
   config={"integration_spec": {
       "name": "x",
       "components": [APPLIABLE_CONN],
       "runtime": {"password_zq9": "canary-157-runtime"},
   }}
   ```

   `build_integration_action(..., action="plan")` returns the secret-shaped key and synthetic value in its preview. The same specification through `authoring_request.intent_kind="integration_spec"` correctly returns `PLAINTEXT_SECRET_REJECTED`.

   This is pre-existing behavior that the requested hardening leaves unimplemented. None of P1–P9 or D1–D19 exempts raw runtime secrets. Apply the shared scanner at the common intake boundary, before normalization and echoes.

2. **ARCH-157-e3-02 — Standard, runtime behavior: the single binding decision still does not reach every consumer.**

   Two cases remain.

   **A known reused profile can lose its required sentinel.** `_validate_processes` obtains reuse membership, but [resolve_selected_profile_artifacts](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:5589) performs another binding lookup. If that lookup fails, [line 5603](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:5603) skips the profile before inserting `None`.

   Reproduction: the first lookup binds a target by name; its account index contains an additional required leaf; the second lookup fails transiently. Planning reports zero errors and serves a **generator** summary without the account-required field. The candidate has become evidence for a profile already known to be reused.

   **Recipe normalization still judges a map that will be reused by name.** The [recipe gate](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/recipes/engine.py:1749) uses declared-only bindings before account-aware validation. With `reference_only=true`, an unambiguous existing map name, no declared ID, and incomplete discarded candidate mappings, direct authoring passes while recipe intent fails with `RECIPE_CONSTRAINT_FAILED` and `TRANSFORM_REVIEW_REQUIRED_TARGET_UNMAPPED`.

   The structural correction is incomplete: carry resolved binding identities with reuse membership, preserve every known reused key when evidence retrieval fails, and defer recipe judgments that require unresolved account bindings.

3. **ARCH-157-e3-03 — Standard, machine-served contracts: derived flows still contain discarded metadata and incorrect final names.**

   The selected-artifact model now correctly excludes a candidate generation body, and reused maps are opaque. Two other row fields remain outside that correction.

   **DB required flags:** [_db_schema_summary](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/derived_flows.py:247) reads `required`/`mandatory` from candidate configuration. Its [caller](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/derived_flows.py:510) supplies that configuration even for selected artifacts. A successful plan reproduced this contradiction:

   ```text
   evidence_source: selected_artifact
   source_profile_generation.field_index_by_path.source_field_a.required: false
   source_schema.fields[0].required: true
   ```

   **Clone names:** [_generated_profile](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/derived_flows.py:335) uses the original normalized name. With a collision and `conflict_policy="clone"`, the preview reports `Existing Target`, while the [apply naming authority](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:10627) produces `Existing Target-clone`.

   Item 7 requires the entire projection to describe selected content and final component naming. Both subshapes must consume those authorities.

4. **ARCH-157-e3-04 — Standard, runtime behavior: a literal watermark profile reference can remain unresolved through compilation.**

   The [literal-profile resolver](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:5495) collects map endpoint IDs. A UUID referenced only by a watermark is never discovered. The watermark validator [returns `False`](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/governance.py:1055) while evidence remains unavailable, and its [final caller ignores that return](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/workflow.py:1961).

   A public compile request with a literal source-profile UUID and field `updated_at` succeeded with zero errors, a compile hash, one unresolved deferred watermark, and **no profile read**.

   Selected `$ref` watermark cases are corrected. Literal references still need discovery and a conclusive final validation result. The existing [literal-deferral test](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_157_plan_conformance.py:1460) does not establish that compilation eventually resolves the declaration.

5. **ARCH-157-e3-05 — Standard, blocking served-contract compatibility evidence: the accounting corrections are not wired through the real discharge route.**

   **Profile names:** [canonical replay normalization](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_flows_accounting.py:625) removes `component_name`; the frozen normalized payload also lacks it. Consequently, the [new name checker](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_flows_accounting.py:678) skips the comparison. Replacing both actual generated profile names with `UNRELATED WRONG PROFILE NAME` still makes the real replay plus `discharge_case()` return `ok=True`.

   **Retirement applicability:** the helper checks field applicability only when `field_path` is supplied. Neither the [flow discharge caller](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_flows_accounting.py:711) nor the [endpoint caller](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_flows_accounting.py:777) supplies it. An API send row or endpoint can therefore be “retired” using the unrelated JDBC-options record `RET-157-01`.

   RETAIN/SPLIT classification rejection is fixed. No committed row currently uses retirement discharge, so the existing nine discharges are not overturned. The required reusable mechanism remains incomplete.

   The new [witnesses](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_157_plan_conformance.py:1281) exercise helpers with synthetic name-bearing payloads and explicitly supplied paths. Their mutation sensitivity does not prove that production accounting supplies that evidence.

6. **ARCH-157-e3-06 — Standard, machine-served contracts: the retirement fingerprint overclaim remains public.**

   The workflow-contract paragraph was corrected, but the [actual obsolete-spelling refusal](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/models/governance_intent.py:243) still says the metadata never affected “a fingerprint.” The [taxonomy summary](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/errors.py:2401) repeats that claim. Public validation translation preserves the refusal message.

   The records acknowledge full-echo hash churn and establish non-applicability of canonical execution/materialization fingerprints. They do not support universal fingerprint invariance. Correct the remaining served messages to the same measured scope as the workflow contract.

The requested earlier-finding reconciliation is:

| Finding | Status on this tree | Code-supported assessment |
|---|---|---|
| **e1-01 — selected-artifact coverage** | **Partial** | Shared policy/predicate resolution and refresh coverage exist; finding e3-02 still permits candidate fallback and recipe inconsistency. |
| **e1-02 — derived-flow fidelity** | **Partial** | Selected-artifact summaries and reused-map opacity are implemented; e3-03 and the accounting-name gap remain. |
| **e1-04 — watermark evidence** | **Partial** | Selected `$ref` cases now resolve correctly; literal references can remain undecided. |
| **e1-06 — retirement proofs** | **Closed** | Actual ordered create-document traces and measured fingerprint non-applicability now provide the previously missing evidence. |
| **e2-01 — unavailable evidence/recipe reuse** | **Partial** | Successful binding followed by failed index discovery retains `None`; failed repeated binding and name-only recipe reuse remain uncovered. |
| **e2-02 — mixed selected/candidate flows** | **Partial** | Candidate generation bodies and reused-map mappings are excluded; DB summaries and final clone names remain incorrect. |
| **e2-03 — watermark decisions** | **Partial** | Candidate-versus-selected `$ref` decisions are corrected; final literal-reference deferral remains. |
| **e2-04 — accounting guarantees** | **Partial** | Classification checks work; real-route name comparison and retirement applicability do not. |
| **e2-05 — fingerprint evidence/claims** | **Partial** | Applicability evidence is implemented; the public refusal and taxonomy still overclaim. |

The previously closed **e1-03, e1-05 and e1-07 remain closed**: connection identity checking is shared across create/reuse branches; competing `flows` locations receive named refusal; advisory and authoring routes share the required-target implementation and destination interpretation.

Coverage of every numbered plan item:

| Item | Status | Assessment |
|---|---|---|
| **1. Audit and historical evidence** | **Implemented** | Literal baseline, advance roster, consolidated shipped contracts/handoff, isolated baseline extraction, seven-producer census and immutable provenance exist. Corpus: 21 cases, 83 flows and 58 endpoint/provenance rows. |
| **2. Authored defaults and envelopes** | **Implemented** | Strict resolved name, explicit action, authored prefix defaults, structural validation, collision checks, typed recipe envelopes and equivalent-default fingerprints are implemented. Consumed defaults leave the executable representation. |
| **3. Dependency ownership/fan-out** | **Implemented otherwise** | Reference/dependency ownership, root boundaries and explicit governance are respected. D8/D9’s late folder-ID injection is justified by the settled platform behavior. The trading-partner name-only route remains an explicitly reported limitation. No relocation mutation was introduced. |
| **4. Secrets and connection contract** | **Partial** | Shared scanner, mutable typed-intake recheck, binding XOR, identity checks, shared REST policy and strict header merger exist. Raw runtime scanning remains missing. P1’s diagnostic-family change is reasonable. |
| **5. One required-target validator** | **Implemented** | The sole [coverage implementation](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/builders/transform_map_validation.py:326), shared destination interpreter, six legacy replacements and advisory integration exist. JSON/DB authorities and empty-index deferral are preserved. |
| **6. Entry routes/selected artifacts** | **Partial** | All six required entries reach the shared validator. Selected binding and recipe timing gaps prevent the promised artifact-authority guarantee. |
| **7. Typed derived flows** | **Partial** | Typed preview, output-only refusal, rich normalization and executable-hash exclusion work. Selected-content and final-name fidelity remain incomplete. |
| **8. Persistent accounting** | **Partial; justified P5 deviation** | Nine rows discharged, zero retired, 132 explicitly pending for #159. Canonical replay, generative differentials and ordered two-sided accounting exist, subject to e3-05. The pending state reasonably resolves the plan’s sequencing conflict. |
| **9. Recorded-not-wired channel** | **Implemented** | Fifth typed contribution, identity/ownership checks, deterministic output, direct results and persisted build provenance exist outside executable hashes and constraint inputs. D6’s legacy-response omission is justified. |
| **10. Watermark/scheduling/retirements** | **Partial** | Typed declarations, query consistency, persisted-DPP separation, named obsolete-input refusals and eight RETIRE/one RETAIN/one SPLIT records exist. Literal watermark validation and retirement wording remain open. |
| **11. Served contracts/artifacts** | **Partial** | Registries, selectors, taxonomy and generated artifacts are updated. P1–P3 are justified adaptations. Remaining served inaccuracies are blocking. ProcessIR schema, XML goldens, dependencies and CI workflows are unchanged. |
| **12. Test matrix** | **Partial** | Broad coverage and the six-route invocation-removal witness exist. The reproduced cases above remain outside the effective assertions. Consolidating connection tests into other files is acceptable. |
| **13. Public-boundary live QA** | **Implemented historically** | Reports cover both intents, supporting placement, extensions, update preservation, multiple roots, fingerprints, verification and exact cleanup. This review performed no additional live QA. |
| **14. Completion/scope boundaries** | **Partial / validation owed** | Twelve commit-review archives exist; the latest code delta passed review, followed by archival changes. Final-tree Python 3.11/full-suite and composite wave evidence remain unestablished. Downstream ownership boundaries are otherwise preserved. |

**Governance is on typed per-root envelopes, never on `ProcessIRV1`.** The IR model and frozen schema are unchanged.

**Flows are genuinely derived and output-only, and remain outside executable hash/materialization authority.** They do not yet consistently describe only the artifacts this request will bind, because of the binding, DB-summary and clone-name cases above.

**The retirement mutation-proof correction earns acceptance.** The [harness](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_retirements.py:178) captures actual ordered create submissions. Its [applicability measurement](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_retirements.py:219) establishes zero canonical roots on both sides, and the [checker](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/_issue_157_retirements.py:401) rejects missing evidence or newly applicable fingerprints. This closes e1-06 without inventing invariant hashes.

Verification in this review: **467 focused tests passed**, the legacy inventory reported **no drift**, corpus/retirement checks passed, and contract/page-schema artifacts matched their generators exactly. These checks used Python 3.12.13. I found no completed final composite wave gate with `--require-plan-fingerprint`, no final Python 3.11 full-suite evidence, and no later live-validation record establishing coverage of all corrections after QA r13. The ledger’s final-tree validation table remains empty.

This ends the third architect evaluation. The correction batch needs its affected QA, a clean Stage-2 correction review and current final wave validation. The Critical finding prevents closure while unresolved; no fourth architect evaluation is required.

VERDICT: ISSUES FOUND

