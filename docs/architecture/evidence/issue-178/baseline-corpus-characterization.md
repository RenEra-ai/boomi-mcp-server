# Baseline characterization of the #178 regression corpus

Measured read-only at the step-0 baseline `cdd7a3bf8e2e7ae6773f6ec4250844e5c2e8cf8f`, from a clean
tree, BEFORE any source edit — a characterization taken after the change is not a characterization.
Every triple below is quoted from probe output, never re-keyed from memory. All five rows
REPRODUCED, including the two that #175 carried as reviewer-reported and the one (`row 3`) that
#175 could not construct at all.

Provenance marker for every row: *measured here*.

## The five corpus rows

### Row 1 — `branch-cache-prefix-process-call-terminal`
Branch leg `steps=[cache_put]`, `terminal=process_call`. **CONFIRMED — all three fields diverge.**

- Parser: `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` at `/body/steps/0/legs/0` —
  *"a trailing cache_put belongs in the leg terminal (target-less staging leg), not in steps"*
- Compiler (`validate_body_capabilities` and `compile_process_ir_v1`, byte-identical):
  `PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED` at
  `/body/steps/0/legs/0/terminal` — *"a process_call branch leg terminal admits no preceding
  steps — a call whose child returns no documents ends the path it is on, and a prefix before it
  is not attested"*

Mechanism: `BranchLegV1._leg_rules` runs the trailing-`cache_put` cardinality raise BEFORE
`_check_process_call_terminal_form`, so the parser never reaches the placement verdict.
Control B (`steps=[set_dpp]`, `terminal=process_call`) makes both paths agree exactly, so the
placement rule itself is not the divergent component.

### Row 2 — `root-branch-then-process-call`
Root body `steps=[branch, process_call]`. **CONFIRMED.**

- Parser: `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` at `/body` — *"no step may follow
  a branch or decision — control nodes are terminal fan-out in ProcessIR v1
  (continuation_after_branch_or_decision is gated)"*
- `validate_body_capabilities`: ACCEPTED (yields silently, 0 diagnostics)
- `compile_process_ir_v1`: `PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` at `/body/steps/0` — *"the CFG must
  have exactly one entry node"*. Identical with `symbols=None` and with a resolving symbol table.

Kind-agnostic: replacing the second root step with `set_dpp` yields byte-identical triples on all
three paths, so this is a control-continuation defect, not a process-call placement defect.

### Row 3 — `root-process-call-then-source`
Root body `steps=[process_call, source]`. **CONFIRMED — independently reproduced for the first
time**; #175 recorded it as reviewer-reported and could not construct it.

- Parser: `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` at `/body` — *"source may appear only as the
  first step"*
- Compiler (`validate_body_capabilities` and `compile_process_ir_v1`):
  `PROCESS_IR_CAPABILITY_UNSUPPORTED` at `/body` — *"a process_call may not share a root-to-leaf
  path with a connector step (process_call_connector_mixing is gated)"*

Same pointer, different code and message. Node payloads that reproduce it:
`{"kind": "process_call", "process_ref": "child_process"}` and
`{"kind": "source", "connection_ref": "conn_rest", "operation_ref": "op_rest_get"}` — the `source`
endpoint requires BOTH refs, which is what defeated #175's probe.
Order-sensitive: reversing to `[source, process_call]` makes both paths return the identical
`PROCESS_IR_CAPABILITY_UNSUPPORTED` triple. Only the `[process_call, source]` ordering diverges.

### Row 4 — `process-try-process-call-first-step`
Process-scoped `try_body` whose first step is a Process Call. **CONFIRMED.** Base fixture:
`tests/fixtures/process_ir/error_handling/scoped_try_catch_process_retry0_exception.json`.

- Parser: `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` at `/body/steps/0/try_body/steps/0` —
  *"this node kind is not admitted in this control-body slot"*
- Compiler: `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` at `/body/steps/0/try_body/steps` —
  *"a process-scoped try body must begin with the connector_call that produces the flow's
  documents"*

Different code AND different pointer depth: the parser's pointer carries the step index, the
compiler's is index-free. **This is the one row where the parser's answer is arguably WORSE** — the
specific process-scope rule is replaced by the generic slot message, while the pointer becomes more
precise. Recorded as a deliberate cost of the parser-authority decision, not an unnoticed
regression.

### Row 5 — `root-connector-branch-process-call-terminal`
Root `connector_call`, then a Branch whose leg 0 terminal is a Process Call. **CONFIRMED — and the
#175 wording is ambiguous, so BOTH readings were measured and both are pinned.**

- **No-prefix reading** (`leg0.steps=[]`): same code, same pointer, MESSAGE-ONLY divergence.
  Both: `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` at `/body/steps/1/legs/0/terminal`.
  The parser's message carries the clause *" — a connector runs upstream of this body"*; the
  compiler's omits it.
- **Prefix reading** (`leg0.steps=[set_dpp]`): a CODE divergence, not a message one.

Both readings enter the corpus as separate pinned cases; collapsing them would lose a defect.

## Superset defects — the issue's acceptance criterion 4 premise is FALSE

#178's body states the safety property *"no document is accepted by one entry point and refused by
the other"* **already holds** and merely needs to become a test. Measurement refutes that. The
compiler is not a superset refuser; there are documents the parser REFUSES and the compiler
ACCEPTS and fully COMPILES:

| Document | Parser | Compiler |
| --- | --- | --- |
| Branch leg `steps=[cache_put]`, `terminal=stop` | REFUSED `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` at `/body/steps/0/legs/0` | ACCEPTED — compiles to a real emission plan (`start_noaction`, `branch`, `doccacheload`, `stop`, `setproperties_step`, `stop`) |
| Root `[set_dpp, source, target, stop]` (source out of position, no `process_call`) | REFUSED *"source may appear only as the first step"* | ACCEPTED — compiles clean into a 4-node CFG / 5-shape plan |
| A Branch with ONE leg | REFUSED `PROCESS_IR_SCHEMA_BRANCH_CARDINALITY` at `/body/steps/0/legs` | `validate_body_capabilities` accepts; the full pipeline crashes to `PROCESS_IR_COMPILE_INTERNAL` with an EMPTY path |
| `ir.version = "2"` on an otherwise legal document | REFUSED `PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED` at `/version` | ACCEPTED by `validate_body_capabilities` AND `compile_process_ir_v1` — no compiler stage reads `version` at all |

So the compile-entry re-parse does not merely rename codes: it closes a real accept-direction hole,
converting silent mis-compiles into refusals. That is a behaviour change beyond diagnostic identity
and is recorded here rather than discovered at review.

The other direction is also not a superset: 41 of 154 census documents are refused by the compiler
and accepted by the parser (e.g. `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED`,
`PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`, `PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`), because
compiler-only codes need a symbol table the parser does not have. The safety property this slice
can pin is therefore **grammar acceptance at the compile entry**, not full-compile acceptance.

## Measured blast radius

Exactly TWO existing diagnostic-identity assertions change, measured under a translated,
`functools.wraps`-preserving in-process shim (28 files: baseline 2264 passed / 16 skipped ->
2262 passed / 16 skipped, 2 failed):

1. `tests/test_process_ir_rich_control_bodies.py::test_the_root_verdict_yields_to_the_control_continuation_rule`
   — corpus row 2; the test's own docstring argues the parser's answer is the correct owner.
   **Superseded, not broken.**
2. `tests/test_process_ir_error_handling.py::test_mutated_away_catch_terminal_is_rejected_by_the_compiler`
   — **a genuine regression, not a supersession.** See below.

Two further tests in `tests/test_process_ir_semantic_emission_gate.py` assert on the REAL
`inspect.getsource(compile_process_ir_v1)` token order and were never exercised by any shim
(`functools.wraps` makes `inspect.getsource` return the ORIGINAL source — measured). They must be
re-verified against the landed edit.

## The catch-terminal regression (must be fixed in-slice, not shipped)

`ir.body.steps[0].catch_body.terminal = None` today yields
`PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED` at `/body/steps/0/catch_body/terminal` — *"the catch body
does not reach a terminal"*. After a naive re-parse it degrades to the generic
`PROCESS_IR_SCHEMA_INVALID` — *"value does not match the strict ProcessIRV1 schema"*.

Root cause, measured: the dump renders `"terminal": null` with the key PRESENT, so pydantic reports
`err_type='model_attributes_type'`, not `'missing'`. The dedicated branch in
`src/boomi_mcp/models/process_ir.py` only matches the `missing` form, so it never fires. Control:
with the key ABSENT the parser DOES return `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED`.

This is served, machine-facing contract text (the code keys
`authoring_contract_entry_ids_for_diagnostic`), so it sits in the
`machine-served schemas/contracts` blocking class and is a strict LOSS. Fixed in-slice by widening
that branch to match the null form as well as the missing form.

## Legacy re-parse safety

Measured three independent ways; no legacy, materialization or recipe path fails the re-parse:

- Full non-KB instrumented census over 154 distinct IR documents reaching `compile_process_ir_v1`:
  cross-tab `(OK, REPARSE_OK)=106`, `(RAISED, REPARSE_OK)=41`, `(RAISED, REPARSE_RAISE)=7`, and
  critically **`(OK, REPARSE_RAISE)=0`** — no document exists that the compiler accepts and a
  re-parse refuses.
- Per dialect: `flow_sequence` 33 documents / 0 re-parse failures, `sync_pipeline` 4 / 0,
  `wrapper_subprocess` 4 / 0.
- Exemptions genuinely fire (11 measured `apply_policy` downgrades on `flow_sequence`), and all 166
  policy-carrying compiles in that run re-parsed OK.

Residual gap, recorded rather than closed: a legacy config shape no test exercises could still
produce a `ProcessIRV1` the parser refuses. This is coverage, not proof over all legacy configs.

## Exemption / parser code disjointness

`_EXEMPT_CODE` downgrades exactly four canonical codes — `PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING`,
`PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN`, `PROCESS_IR_SEMANTIC_LINEAGE_PROPERTY_READ_BEFORE_WRITE`,
`PROCESS_IR_SEMANTIC_SIDE_EFFECT_ORDERING_UNKNOWN` — and none appears in the 15 codes
`process_ir_v1_parse_diagnostic_specs()` enumerates. The intersection is empty, so a compile-entry
re-parse can never suppress or bypass a legacy exemption. Pinned as a derived invariant test rather
than left as prose.

## Cost

Re-parse median `0.0472 ms` on the largest real in-tree IR (`control_flow`, 12 nodes, 1145
canonical bytes, 400 reps after 5 warmups); the dump half is ~10%, the parse half dominates.
Relative to a resolving compile: flat **8.8%–13.2%** from 1 to 552 nodes. End-to-end typed
compile+apply with only the network faked: **+2.4% to +3.5%**. Amplifier, measured with stack
attribution: `compile_process_ir_v1` runs **6x per process root** per typed compile+apply pair,
scaling linearly with root count. No timing assertion is added — the measurement is recorded, not
enforced.

## Case-set constructibility

`BODY_CAPABILITIES_V1` is a total 10-row mapping (5 contexts x 2 slots, no gaps). The closed kind
union has **20** members (`sequence` excluded from candidates leaves 20 authorable kinds; the "18"
recorded in older notes is stale). Base grid 5 x 2 x 20 = 200 cells, of which 70 are admitted
(exactly the row sum, verified) and 130 refused. `PROCESS_IR_V1_MAX_CONTROL_DEPTH` is 2. A generic
`{kind -> model class}` map derives from the union itself for all 20 kinds; 17 of 20 need 0–2
scalar fields, and only `decision`, `try_catch` and `data_process` need structure.
