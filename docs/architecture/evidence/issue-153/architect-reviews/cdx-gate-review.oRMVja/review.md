I’m treating this as a read-only fourth architect gate. I’ll first reconstruct the exact AR1–AR3 finding definitions from the audit record, then verify each against the current tree and independently hunt for new blocking-class defects. I’ll keep any probes away from unpatched client paths.The repository is clean at `e7bbad3a9182b627982643dcb6b8d77d1c17bb30`. I’ve recovered the historical finding texts and am now checking the present implementation, with separate passes over AR1, AR2, and AR3 while I concentrate on mutation ordering, plan/bundle binding, late ID binding, and served contracts.The historical fixes are largely present, but one important recheck has failed: the compiler revision still does not bind the execution-profile derivation rule itself. Its manifest includes only the two profile labels and listener-family set, so replacing the derivation algorithm leaves the served revision unchanged. I’m validating that independently and checking whether the plan model has the same “self-consistent but semantically incomplete” hole in its recorded symbol slots.I’ve finished the 30-item trace and reproduced two boundary conditions directly. I’m doing the final mutation-accounting check now, then I’ll run the focused regression set and return the complete status matrix.The focused suite is green: 196 tests passed. The remaining question is not ordinary regression coverage—it is contract congruence and mutation evidence on two adversarially constructed states, both of which the current models/routes accept.Issues remain at current HEAD `e7bbad3a9182b627982643dcb6b8d77d1c17bb30`: 29 historical findings are resolved, AR3-07 is partially resolved, and I found two new defects.

## Blocking findings

1. **AR3-07 — PARTIALLY RESOLVED, Critical (served contract).**  
   The compiler revision now includes profile names and listener connector types at [contract.py:879](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/contract.py:879), but not the actual derivation rules implemented at [execution_profile.py:54](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/execution_profile.py:54) and [execution_profile.py:85](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/compiler/process_ir/execution_profile.py:85). Replacing `derive_process_execution_profile` with an always-scheduled implementation left `_compiler_revision()` unchanged at `sha256:c79ea7…521a`. A behavioral change can therefore retain a stale-valid served revision binding.

2. **NEW-01 — Critical (mutation accounting): successful create without a result ID loses confirmed-write evidence.**  
   After the raw create succeeds, `component_id` may remain `None` at [integration_builder.py:7966](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:7966). The immediate write callback is incorrectly conditional on that ID at [integration_builder.py:7978](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:7978). The later fail-closed error is correct at [canonical_process_apply.py:328](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/canonical_process_apply.py:328), but the partial envelope only emits nonempty write evidence at [integration_builder.py:8774](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/integration_builder.py:8774).

   A fully network-patched public probe returned `PROCESS_MATERIALIZATION_RESULT_ID_MISSING` with a durable `failed_partial` build, but both `process_writes` and `process_mutations` were absent. The platform-confirmed write therefore has no reconciliation record.

3. **NEW-02 — Standard: the plan model does not validate symbol-slot completeness.**  
   `unresolved_symbol_slots` defaults to empty at [process_materialization.py:288](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_materialization.py:288). Validation checks ordering, relocatability, and fingerprint consistency, but never compares the slots with references derived from the ProcessIR at [process_materialization.py:302](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/authoring/process_materialization.py:302). Apply trusts that recorded inventory at [canonical_process_apply.py:234](/Users/gleb/Documents/Projects/Renera/boomi-mcp-server/src/boomi_mcp/categories/components/canonical_process_apply.py:234).

   A production-built plan had four slots; reconstructing it with zero slots and its correctly recomputed fingerprint passed normal model validation. Production construction derives the slots correctly, so this does not reopen AR1-03, but the required plan-level disagreement guard is absent.

## Historical finding matrix

### AR1

| Finding | Status | Current evidence |
|---|---|---|
| AR1-01 | RESOLVED | Plans are retained in the compiled bundle and checked/consumed during apply: `authoring/workflow.py:2012-2027, 2173-2207, 2416-2424`; `categories/integration_builder.py:7858-7892`. |
| AR1-02 | RESOLVED | Envelope and ProcessIR references share relocatability enumeration and are rejected before compilation/write: `authoring/process_materialization.py:223-268, 565-574`; `integration_builder.py:8612-8637`. |
| AR1-03 | RESOLVED | Production construction derives ordered typed slots; late binding uses those slots: `process_materialization.py:459-510, 579-605`; `canonical_process_apply.py:234-246`. |
| AR1-04 | RESOLVED | Durable build allocation precedes mutation; partial and exceptional exits retain the build and evidence: `integration_builder.py:8717-8791, 9019-9030, 10043-10108`. |
| AR1-05 | RESOLVED | Connected-account scope and exact pre-call create/update XML digests are used: `integration_builder.py:3255-3262, 3292-3298, 7957-7964, 8855-8864`. |
| AR1-06 | RESOLVED | Exact unique nondeleted placement resolution runs pre-write; effective placement is attested: `integration_builder.py:7509-7552, 8054-8143, 8695-8715`. |
| AR1-07 | RESOLVED | Canonical version-1 material, nested policy/revision groups, and explicit exclusions are present: `process_materialization.py:73-86, 175-210, 371-416`. |
| AR1-08 | RESOLVED | Explicit recipe fields, tuple processes, deep reparsing, and named error mapping are enforced: `authoring/workflow.py:359-431, 731-762`; `integration_models.py:88-114`; `integration_builder.py:7353-7383`. |
| AR1-09 | RESOLVED | Changed capabilities are version 2/supported and served guidance describes direct apply: `authoring/contract.py:143-181, 563-590`; `categories/meta_tools.py:9964-9973`. |
| AR1-10 | RESOLVED | Canonical envelope golden exists and the public-chain independence witness bombs legacy builders: `tests/fixtures/wave_gate/goldens.jsonl:62`; `tests/test_process_component_materializer.py:241-348`. |

### AR2

| Finding | Status | Current evidence |
|---|---|---|
| AR2-01 | RESOLVED | Direct and integration-spec units are reparsed; warning-bearing dumps are suppressed: `authoring/workflow.py:359-431`; `semantic_validation/context.py:145-151`. |
| AR2-02 | RESOLVED | Relocatability, plan construction, and dry emission occur before writes; execution consumes the cached plan: `process_materialization.py:565-577`; `integration_builder.py:7652-7674, 7875-7889, 8601-8670`. |
| AR2-03 | RESOLVED | Known-ID confirmed writes are recorded before downstream work and retained on returned/raised failures: `integration_builder.py:7966-7990, 8717-8791, 8821-8846, 10082-10107`. NEW-01 covers the distinct missing-ID edge. |
| AR2-04 | RESOLVED | Update attestations use effective preserved placement: `canonical_process_apply.py:352-403`; `integration_builder.py:8109-8124`. |
| AR2-05 | RESOLVED | Fingerprint material contains the direct full policy projection: `process_materialization.py:401-411`. |
| AR2-06 | RESOLVED | Structural strings reject surrounding whitespace instead of silently normalizing it: `models/process_component.py:74-99, 127-138, 209-226`. |
| AR2-07 | RESOLVED | Pydantic/cardinality failures map to reachable issue-owned codes: `integration_builder.py:7360-7383, 9683-9726`; `tests/test_issue_153_canonical_apply_e2e.py:3306-3346`. |
| AR2-08 | RESOLVED | Composer emits one diagnostic per ProcessIR cause with its authoritative path: `recipes/composer.py:286-319`. |
| AR2-09 | RESOLVED | Served descriptions correctly distinguish the legacy component list and canonical process tuple, with an executable guard: `integration_models.py:73-112`; `tests/test_integration_models.py:118-175`. |
| AR2-10 | RESOLVED | Independence coverage drives public compile→apply with network and legacy boundaries patched: `tests/test_process_component_materializer.py:241-348`. |

### AR3

| Finding | Status | Current evidence |
|---|---|---|
| AR3-01 | RESOLVED | The `integration_spec` arm reparses every nested unit before compilation: `authoring/workflow.py:359-431`. |
| AR3-02 | RESOLVED | Raw plans are built, cached, and dry-emitted in the pre-write pass: `integration_builder.py:7572-7674, 7875-7887, 8584-8670`. |
| AR3-03 | RESOLVED | For the reported known-result-ID case, the callback records the write before readback/attestation and propagates it on both failure paths: `integration_builder.py:7966-7990, 8774-8779, 8821-8846, 10082-10107`. |
| AR3-04 | RESOLVED | Both integration collections now have accurate, noncontradictory served descriptions: `integration_models.py:73-114`. |
| AR3-05 | RESOLVED | Served workflow and vocabulary name units, relocatable plans, late binding, and separate attestations: `authoring/contract.py:563-639`. |
| AR3-06 | RESOLVED | Zero-step warning checks the complete participant universe: `integration_builder.py:7127-7134`. |
| AR3-07 | **PARTIALLY RESOLVED** | Constants are projected at `authoring/contract.py:879-899`; the behavioral rules at `execution_profile.py:54-123` remain invisible to the revision. |
| AR3-08 | RESOLVED | Slot references use `ComponentRefV1`; expected types are validated and nonempty: `process_materialization.py:106-172`; `models/process_ir.py:205-231`. |
| AR3-09 | RESOLVED | All three revision fields use `DigestString`: `process_materialization.py:292-298`; `models/authoring_workflow.py:107-111`. |
| AR3-10 | RESOLVED | Materializer revision includes the canonical full preservation policy: `integration_builder.py:7285-7333`; `process_materialization.py:196-211`. |

Focused validation completed with **196 tests passed** across the #153 model, plan, materializer, DAG, canonical-apply, and end-to-end suites. The worktree remained clean and unchanged.

VERDICT: ISSUES FOUND
