# Stage-1 QA round 1 — issue #178

Loop 1 (Stage-1 QA), evaluation 1. Live, through the public MCP tool boundary, against the
`renera` account. **Verdict: CLEAN, zero findings.**

- Tree tested: `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f` plus the uncommitted working tree.
  Freeze stamp `cdd7a3bf8e2e/dirty:2bbf52bd8fe0/code=65f521bce7b4`, stable PRE -> POST -> EXIT
  across every scenario.
- Suite, spent once at HEAD: **10209 passed, 17 skipped, 0 failed** — *measured here*.
- Account and repo tree byte-identical after the round: 22 processes / 24 connections /
  22 operations, zero `_TEST_QA178*` residue; every provisioned component deleted.
- Fixture provenance (2026-08-14 amendment): node and envelope shapes from the SERVED
  `get_schema_template(schema_name="AuthoringRequestV1")`, `schema_version 2`,
  `schema_hash sha256:741fac168e35e39ad…`; legacy config from the served `wrapper_subprocess`
  protocol template; archetype parameters from each archetype's own served
  `examples[*].parameters`; compose requests from `examples/m8/*.integration.json` frozen
  2026-07-11; component ids from live `query_components`. Nothing derived from the slice's own
  test file, the compiler under test, or slice-generated goldens.

## The methodological result — recorded because it narrows what this slice's live QA can prove

QA established, by measurement rather than by a clean differential, that **most of the parity
change is not observable at the MCP tool boundary**, and said so instead of banking a green run:

1. **Scenarios A and C are refused at the `AuthoringRequestV1` INTAKE parse, not by the new
   compile-entry re-parse.** A full leaf-diff of all 22 served envelopes, baseline versus HEAD,
   shows ZERO behavioural difference; the only differing leaves are `capability_revision` and
   `plan_hash` (source-digest drift, expected) plus probe metadata. All eight rows serve identical
   `(code, pointer, message)` at BOTH SHAs. They meet their acceptance criteria — none accepted,
   every one carrying a non-empty `error_code` — but they do **not** discriminate this change.
2. **Four public arms that emit real process XML never enter `compile_process_ir_v1` at all**:
   `database_to_api_sync` (emit + apply), `http_listener_to_db`, `http_listener_to_rest`, and
   `api_to_database_sync` apply. Their clean A/B was vacuous. Verified two independent ways — an
   env-gated raise seeded at the changed entry, and a stack census run on BOTH trees. Reachability
   is identical at `cdd7a3b` and at HEAD, so this is a pre-existing repo fact, not a slice change.

This is the correct characterization rather than a defect, and it matches what #178 actually
describes: the hole is reached *"by mutating an exported `ProcessIRV1` (the model is exported AND
mutable) and handing it straight to the compiler"* — i.e. by a DIRECT API caller. The tool boundary
re-parses at intake already (`authoring/workflow.py::_reparsed_unit`), so a tool-boundary caller
could never reach the divergence. Live QA therefore proves **no regression** at that boundary plus
the one improvement below; the parity property itself is proven by the derived gate in
`tests/test_process_ir_entrypoint_diagnostic_parity.py`, which exercises the direct compiler API
where the defect actually lives.

## Scenario results

- **A — served diagnostic identity: 5/5 exact.** Every row served the expected parser code,
  `_success: false`, `error_code: "INVALID_INPUT"` (non-empty), `mutation_performed: false`, on both
  `plan` and `compile`. Pointers matched with the intake prefix, e.g. A5 ->
  `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` at
  `/intent/units/0/process_ir/body/steps/1/legs/0/terminal`.
- **C — all three refused.** `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` (twice) and
  `PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED` at `/version`. None accepted — and all three were already
  refused identically at the baseline, per point 1 above.
- **B — no mutation on refusal, WITH a positive control.** Refusals at `action="apply"` held
  inventory at 22 -> 22 with `mutation_performed: false`. Five genuine creates in the same session
  moved it 22 -> 27, so the probe demonstrably detects mutation rather than being blind to it.
- **D — happy path green end to end.** plan -> compile -> apply created a real component emitting
  deployable XML: `start -> <processcall processId="…" wait="true" abort="false">` with empty
  `returnpaths` (the #175 terminal form).
- **E — legacy dialect unaffected, policy-bearing case measured.** Both arms that reach
  `legacy_adapters/emission.py::emit_legacy_result` do so WITH a validation policy
  (`wrapper_subprocess` apply, `api_to_api_sync`/sync_pipeline apply) and both succeed.
  `compose_archetypes` reproduced both frozen 2026-07-11 M8 oracles byte for byte. Normalized
  emitted-XML sha256 is identical at baseline and HEAD for all five processes, including the
  `catcherrors` Try/Catch variant. All five call sites of the changed entry are covered live.

## The one boundary-visible improvement, confirmed

`catch_body.terminal` explicitly `null` moved from `PROCESS_IR_SCHEMA_INVALID` to
`PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED`, with message, remediation and
`authoring_contract_entry_ids` (`diagnostic.process_ir_semantic_catch_unterminated`, resolving
through the published lookup) all moving with it.

The wrong-TYPE discriminator holds under live probing: `terminal: 42`, `terminal: "stop"` and
`catch_body: 42` all correctly keep `PROCESS_IR_SCHEMA_INVALID`. That is the distinction the fix
turns on — keying the widened branch on the error type alone would have relabelled a wrong-typed
terminal as "does not reach a terminal".

## Secrets / leak sweep

Canaries planted in every string slot. Refusal envelopes leak zero authored values and zero
internal markers, with a positive control proving the sweep is not blind.

## Pre-existing observation, NOT a #178 defect and deliberately not filed

`$.authoring_diagnostics[0].cause_codes[0]` can serve the string `"ProcessIRCompileError"` — a
Python class name in a machine-facing code field — when a `ProcessIRCompileError` escapes the
`authoring/process_materialization.py` call site, which has no local handler. QA first observed this
only at HEAD, then established it as pre-existing by seeding the IDENTICAL line in both trees and
obtaining identical envelopes. Unchanged by this slice, so it takes no disposition here; recorded
because #178 touches that call site and a reviewer may reach it. Per the repo's standing rule, an
accepted pre-existing limitation is recorded rather than minted as debt.

## Instruments persisted (runnable)

Under `.claude/agent-memory/boomi-qa-tester/harness/`: `entry-reachability-census-seed.py`,
`entry-reachability-census-run.py`, `entry-raise-seed.py`, `apply-matrix-emitted-xml.py`,
`canary-internal-leak-sweep.py`, `processir_served_fixtures.py`. Round record:
`.claude/agent-memory/boomi-qa-tester/issue178-rounds-index.md`.
