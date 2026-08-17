I’ll audit the exact baseline-to-HEAD delta against the approved plan, then stress-test the reachability gate, extraction imports, registry behavior, and manifest arithmetic. I’ll keep this read-only and report only actionable plan/acceptance-criteria deviations.The first high-risk area is the freeze gate. I’m checking whether it derives per-kind coverage, rather than merely aggregating emitter keys across a corpus; aggregation can stay green even when one of the 14 legacy kinds silently loses its own lowering path.Found four issues.

- [P1] The deletion gate does not actually freeze each of the 14 `flow_sequence` kinds. It only checks `len(_FLOW_SEQUENCE_ALLOWED_KINDS) >= 14` and aggregates emitter keys across every specimen ([test_issue_151_reachability_freeze.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_issue_151_reachability_freeze.py:384)). A newly allowed kind with no fixture would silently pass; likewise, one kind can lower incorrectly while another still contributes the same emitter key. This violates the “kind added later cannot silently lack a route” criterion. Assert exact fixture-kind equality and attribute emitted nodes to authored kinds via source paths, as the plan specified.

- [P1] None of the four shipped examples is tested through dry-run apply. Both M8 tests stop after `_build_plan` ([test_archetype_composition.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/patterns/test_archetype_composition.py:422), [same file](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/patterns/test_archetype_composition.py:768)); the M11 test also checks planning only ([test_m11_composed_examples.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/tests/test_m11_composed_examples.py:58)). The required dry-run result, planned/applied action equality, and no-client-mutation assertions were silently omitted.

- [P2] The architecture inventory contradicts the final implementation. It says registry discovery has no `__module__` filter ([inventory](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/M12_COMPATIBILITY_INVENTORY.md:2580)) and says `RECIPE_LAYER_MODULES` was not edited and excludes the neutral modules ([inventory](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/docs/architecture/M12_COMPATIBILITY_INVENTORY.md:2684)). Both statements are false after the live-QA fixes, making the #160 handoff misleading.

- [P2] The neutral-module contract is incomplete. `archetype_parameters.py` has no explicit `__all__`, despite the plan requiring a declared reusable export surface ([archetype_parameters.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/patterns/archetype_parameters.py:1)). Also, `_ApiParametersLike` omits `naming` ([archetype_assembly.py](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/patterns/archetype_assembly.py:56)), although `_build_source_response_profile` reads `parameters.naming` ([same file](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/patterns/archetype_assembly.py:560)).

Deviation judgments:

- Rejecting `supported_capability` seeding was justified and non-tautological, but the replacement remains incomplete due to the per-kind gap above.
- The narrower extraction is justified: the retained execution models have no surviving importer, and the moved model/function bodies preserve their AST semantics.
- The implemented deletion-route derivation is correct; `sync_pipeline` and `wrapper_subprocess` survive #160 and must not be classified as deletion routes.
- Both live-QA additions are justified. The registry filter is structurally sound and catalog-neutral; adding the neutral modules to the recipe digest restores coverage. The clause-4 bound is acceptable when combined with its exact-membership and downstream-exclusion pins.
- Manifest arithmetic is correct: `9826 + 11 - 3 = 9834`; the manifest validator passed.

Focused validation passed: 399 tests total across the reviewed suites, plus the wave-manifest check.

VERDICT: ISSUES FOUND
