# QA — issue #175, round "r4 placement-authority re-run"

**Baseline attested at start:** `git rev-parse HEAD` = `2e03be154979430d4408443ad54b303da3251a14`
on `codex/issue-175`, `git status --porcelain` EMPTY, `git grep -n "MUT-" -- 'src/**'`
returned nothing (rc=1).

**TIP MOVED MID-RUN.** During the scenario-3 run the main thread committed
`11ab615136a0944e7b28f433cc0cfc46f207b092` ("Bound the orphan message to what the check
computes and recompute the class ledger"). The freeze guard caught it and VOIDED that run
rather than reporting it. Source delta `2e03be1..11ab615` is one file —
`src/boomi_mcp/categories/components/process_graph_verifier.py`, +11/−2, a served MESSAGE
text change only (no code, no diagnostic code, no control flow). **Every scenario was re-run
at `11ab615` and every result below is the `11ab615` measurement**, freeze-attested
`code_stable=True worktree_moved=False`:

| Scenario | Freeze stamp (attested tip / tree / loaded-module digest) |
|---|---|
| S1 | `11ab615136a0/clean/3264d4e31df5` |
| S1b | `11ab615136a0/clean/c8afdf6c98fe` |
| S2 | `11ab615136a0/clean/3264d4e31df5` |
| S2b | `11ab615136a0/clean/3264d4e31df5` |
| S3 / S3b | `11ab615136a0/clean/3264d4e31df5` |
| S5 | `11ab615136a0/clean/c8afdf6c98fe` |

Every result reproduced identically at both tips (the earlier `2e03be1` runs are in the logs);
the new commit changed no outcome.

**Mode:** LIVE (`BOOMI_LOCAL=true`, profile `renera` → `traininghlibbochkarov-JKIY2X`),
through the public MCP tool boundary. Finding IDs are `QA-175-r4pa-NN`.

---

## Verdict

| Scenario | Verdict |
|---|---|
| S1 verifier narrowing (`PROCESS_CALL_ORPHAN_CONTINUATION`) | **PASS** — 8/8 live arms + 2 non-vacuity controls |
| S2 placement diagnostics, the 5 dispatch-named cases | **PASS** — 10/10 named arms + 3 positive controls agree on both entry points; the 11th arm (C11) is `QA-175-r4pa-01` |
| S2b sibling sweep (the same claim outside the named cases) | **FAIL** — 8/10 arms diverge → `QA-175-r4pa-01`, `-02` |
| S3 dlq + catch_exception plan gate | **PASS** — plan refuses, apply mutates nothing, control moves inventory |
| S3b served remediation | **PASS** with one recorded nuance (no literal `#176`; see below) |
| S5 corrected orphan message (the mid-run `11ab615` commit) | **PASS** — 4/4 with a valid `SHAPE_UNREACHABLE` control |

**Findings: 2.** Both are pointer/code divergences between the two entry points on documents
the dispatch did not name. Neither affects the five cases the dispatch called out, and every
divergent document is still REFUSED by both paths — no document is accepted by one and
refused by the other.

---

## S1 — the narrowed `PROCESS_CALL_ORPHAN_CONTINUATION` rule — PASS

Instrument: `.claude/agent-memory/boomi-qa-tester/harness/175-orphan-rule-scope-matrix.py`
(standing bar, unchanged since r4). Surface: `build_integration(profile="renera",
action="verify", config='{"build_id": ...}')`, which re-reads the STORED component — so
each arm is installed by splicing a full `<shapes>` body into the SAME live component's own
stored envelope (`invoke_boomi_api` POST `Component/<id>/update`, write asserted
`_success=True status=200` before every verify, so no arm can be vacuously clean) and
re-verifying the SAME `build_id`.

Fixture provenance: every wire form is copied byte-for-byte from
`tests/fixtures/live_xml/m11/process_doccacheretrieve_loadalldoc_variant.xml`, a UI-built
live capture frozen 2026-07-03 — causally independent of the code under test. Only
`processId` is retargeted to the live `_QA_FIXTURE_noop` child, and the field under test is
varied per arm.

| Arm | Stored form (read back verbatim from the platform) | Served codes | Verdict |
|---|---|---|---|
| A1 | `<returnpaths/>` + `<dragpoint name="shape10.dragpoint1" toShape="shape2" …/>` | `['PROCESS_CALL_ORPHAN_CONTINUATION']`, `verified=False`, `error_code=PROCESS_GRAPH_INTEGRITY_FAILED` | PASS (fires) |
| A2 | `<returnpaths><returnpaths/></returnpaths>` + dragpoint | `['PROCESS_CALL_ORPHAN_CONTINUATION']` | PASS (fires) |
| A3 | `<returnpaths><returnpaths childShapeName="" returnLabel=""/></returnpaths>` + dragpoint | `['PROCESS_CALL_ORPHAN_CONTINUATION']` | PASS (fires) |
| A4 | populated `childShapeName="shape233"` + dragpoint `identifier="shape233"` | `[]`, `verified=True` | PASS (clean) |
| A5 | populated + dragpoint with **no `identifier`** | `[]`, `verified=True` | PASS (clean — revert confirmed) |
| A6 | populated + dragpoint `identifier="shapeZZZ"` (matches nothing) | `[]`, `verified=True` | PASS (clean — revert confirmed) |
| A7 | populated + 2 dragpoints, one bound one not | `[]`, `verified=True` | PASS (clean — revert confirmed) |
| A8 | `<returnpaths/>` + NO dragpoint (call is terminal) | `[]`, `verified=True` | PASS (clean) |

A5/A6/A7 are the direct measurement that the identifier-correspondence half really is gone:
those three are exactly the arms that FIRED before the revert (r4 measured them firing
against a `git archive 90625b0` extraction). A1–A3 are the same-run positive controls that
the probe can still detect a present defect, so the A4–A8 "absent" results are non-vacuous.

### S1b — the two in-tree m11 captures

Instrument: `harness/175-m11-capture-orphan-check.py`. Measured one layer below the tool
against `verify_process_graph` — because the PLATFORM refuses to store either whole capture
(`Component/<id>/update` → HTTP 400 `ComponentId <guid> …`; they reference doc-cache /
profile / map components that do not exist in this account, re-measured this run and logged
verbatim).

- `process_doccacheretrieve_loadalldoc_variant.xml`: `PROCESS_CALL_ORPHAN_CONTINUATION`
  shapes `[]` — PASS. (The capture is NOT error-free overall — pre-existing
  `BRANCH_OUTPUT_UNSET` on `shape29` and 4× `SHAPE_UNREACHABLE`. Only the per-code
  assertion is meaningful on it.)
- `process_dpp_profile_decision_flow.xml`: orphan shapes `[]`, no error codes at all — PASS.
- **Non-vacuity control 1** (seeded defect): the same capture with shape10's populated
  `<returnpaths>` replaced by an empty one → orphan shapes `['shape10']`. Differential
  `[] → ['shape10']` — PASS.
- **Non-vacuity control 2** (boundary agreement): all eight S1 arm bodies re-verified here
  return the identical code sets the LIVE boundary served — 8/8 agree.

S4 failures: 0.

---

## S2 — placement diagnostics: the five dispatch-named cases — PASS

New instrument: `.claude/agent-memory/boomi-qa-tester/harness/175-placement-authority-agreement.py`.

Both entry points are exercised on the SAME document and compared on code AND JSON pointer
(pointers compared on the tail after `/body`, since path A prefixes the authoring envelope):

- **PATH A — authoring**: `build_integration(profile="renera", action="plan",
  config={"authoring_request": {...}})` with `intent_kind="process_ir"`. Live, non-mutating,
  runs the full intake parse (`parse_process_ir_v1`).
- **PATH B — mutated exported model**: parse a LEGAL document with `parse_process_ir_v1`,
  graft the illegal body onto it (`ProcessIRV1.model_config == {"extra": "forbid"}` — the
  model is exported and NOT frozen, exactly the attack the delta's own comments describe),
  then call the public `validate_body_capabilities`. Confirmed separately that the real
  `compile_process_ir_v1` serves the identical diagnostics (it delegates to that function
  before lowering) — measured for C11, R1 and R4.

Fixture provenance: node field names/shapes read at run time from the served
`get_schema_template("AuthoringRequestV1").json_schema.$defs` (`ConnectorCallNodeV1.required
= ['kind','operation_ref']`, `ProcessCallNodeV1.required = ['kind','process_ref']`, printed
in the log); the placement expectations are the dispatch contract, grounded in the r1 live
m11 measurement; the child and the connector operation are live account components declared
`reference_only`.

| # | Document | Path A (authoring) | Path B (compiler) | Verdict |
|---|---|---|---|---|
| C1 | Decision TRUE arm `steps=[set_dpp]` before terminal `process_call` | `…RETURN_PATH_BINDING_UNSUPPORTED` @ `/steps/0/true_arm/terminal` | identical | PASS |
| C2 | Branch leg 0, same prefix shape | `…RETURN_PATH_BINDING_UNSUPPORTED` @ `/steps/0/legs/0/terminal` | identical | PASS |
| C3 | Decision TRUE arm `steps=[process_call]`, `terminal=stop` | `…RETURN_PATH_BINDING_UNSUPPORTED` @ `/steps/0/true_arm/steps/0` | identical | PASS |
| C4 | Branch leg 0 `steps=[set_dpp, process_call]`, `terminal=stop` | `…RETURN_PATH_BINDING_UNSUPPORTED` @ `/steps/0/legs/0/steps/1` | identical | PASS |
| C5 | Decision TRUE arm `steps=[connector_call]`, `terminal=process_call` | `…NODE_NOT_ALLOWED_IN_BODY` @ `/steps/0/true_arm/steps/0` | identical | PASS |
| C6 | Branch leg 0, same connector shape | `…NODE_NOT_ALLOWED_IN_BODY` @ `/steps/0/legs/0/steps/0` | identical | PASS |
| C7 | Decision TRUE arm `steps=[set_dpp, connector_call]`, `terminal=process_call` | `…NODE_NOT_ALLOWED_IN_BODY` @ `/steps/0/true_arm/steps/1` (the CONNECTOR's index) | identical | PASS |
| C8 | Decision FALSE arm `steps=[]`, `terminal=process_call` | GENERIC `…NODE_NOT_ALLOWED_IN_BODY` @ `/steps/0/false_arm/terminal` | identical | PASS |
| C9 | Decision FALSE arm `steps=[set_dpp]`, `terminal=process_call` | GENERIC @ `/steps/0/false_arm/terminal` | identical | PASS |
| C10 | Decision FALSE arm `steps=[process_call]`, `terminal=stop` | GENERIC @ `/steps/0/false_arm/steps/0` | identical | PASS |
| **C11** | **Decision FALSE arm `steps=[connector_call]`, `terminal=process_call`** | GENERIC @ `/steps/0/false_arm/**terminal**` | GENERIC @ `/steps/0/false_arm/**steps/0**` | **FAIL — `QA-175-r4pa-01`** |
| P1 | CONTROL: Decision TRUE arm `terminal=process_call`, NO prefix | ACCEPTED | ACCEPTED | PASS |
| P2 | CONTROL: Branch leg `terminal=process_call`, NO prefix | ACCEPTED | ACCEPTED | PASS |
| P3 | CONTROL: root `[process_call]` singleton | ACCEPTED | ACCEPTED | PASS |

Every dispatch-named requirement holds: the dedicated code at `/terminal` for a prefixed
terminal call (C1, C2); the dedicated code at the step index for a call in a step slot
(C3, C4); `…NODE_NOT_ALLOWED_IN_BODY` at the CONNECTOR's index for a connector-plus-call
body (C5, C6, C7); the GENERIC code at `/terminal` on both paths for a Decision FALSE arm
(C8, C9); and the bare terminal call ACCEPTED (P1, P2, P3). **The shipping form is not
refused.** `plan` mutated nothing (process inventory 24 → 24).

The matrix is self-controlling in both directions: the ten agreeing arms show the comparison
does not report divergence unconditionally, and the divergent arms show it can detect one.

---

## S2b — sibling sweep: the same claim outside the named cases — 8/10 DIVERGE

New instrument: `.claude/agent-memory/boomi-qa-tester/harness/175-placement-authority-siblings.py`.
Same two entry points, same comparison, sweeping the body contexts and the root slot the
Branch/Decision matrix does not reach.

| # | Document | Path A (authoring) | Path B (compiler) | Verdict |
|---|---|---|---|---|
| T1 | try_body `steps=[connector_call]`, `terminal=process_call` | GENERIC @ `/steps/0/try_body/terminal` | GENERIC @ `/steps/0/try_body/steps/0` | DIVERGE |
| T2 | try_body `steps=[connector_call, set_dpp]`, `terminal=process_call` | GENERIC @ `/steps/0/try_body/terminal` | GENERIC @ `/steps/0/try_body/steps/0` | DIVERGE |
| T3 | try_body `steps=[connector_call, process_call]`, `terminal=stop` | GENERIC @ `/steps/0/try_body/steps/1` | identical | AGREE |
| T4 | catch_body `steps=[connector_call]`, `terminal=process_call` | GENERIC @ `/steps/0/catch_body/terminal` | GENERIC @ `/steps/0/catch_body/steps/0` | DIVERGE |
| T5 | catch_body `steps=[set_dpp]`, `terminal=process_call` | GENERIC @ `/steps/0/catch_body/terminal` | identical | AGREE |
| R1 | root `[process_call, stop]` | `…RETURN_PATH_BINDING_UNSUPPORTED` @ `/steps/1` | same code @ `/steps/0` | DIVERGE |
| R2 | root `[set_dpp, process_call]` | same code @ `/steps/0` | same code @ `/steps/1` | DIVERGE |
| R3 | root `[process_call, set_dpp]` | same code @ `/steps/1` | same code @ `/steps/0` | DIVERGE |
| R4 | root `[connector_call, process_call]` | **`PROCESS_IR_CAPABILITY_UNSUPPORTED`** @ `/body` | **`…RETURN_PATH_BINDING_UNSUPPORTED`** @ `/steps/1` | DIVERGE (**different CODE**) |
| R5 | root `[process_call, connector_call]` | **`PROCESS_IR_CAPABILITY_UNSUPPORTED`** @ `/body` | **`…RETURN_PATH_BINDING_UNSUPPORTED`** @ `/steps/0` | DIVERGE (**different CODE**) |

Two live constraints measured while building the legal skeletons, both of which silently
preempt the rule under test if you get them wrong (recorded in the script):

* a catch body with no steps → `PROCESS_IR_SCHEMA_INVALID_CARDINALITY`;
* a **process**-scoped try body must BEGIN with the `connector_call` that produces the
  documents, and a **connector**-scoped one must END with it → so a try body always contains
  a connector step in either scope. A connector-free try-body prefix is therefore
  UNREACHABLE through the authoring boundary; only the catch body can express one (T5).

---

## QA-175-r4pa-01: the connector-mixing verdict is rendered UNGUARDED in bodies that admit no `process_call`, so the two entry points disagree on the JSON pointer

**Kind**: live-tool defect
**Severity**: Medium
**Blocking class**: machine-served schemas/contracts (the RFC 6901 pointer is machine-facing
and is the diagnostic's answer to "where")
**Tool/Area**: `build_integration` (plan, `intent_kind="process_ir"`) ·
`src/boomi_mcp/compiler/process_ir/body_capabilities.py` `_walk_body` ·
`src/boomi_mcp/models/process_ir.py` `process_call_placement_verdict`
**Affected SHA**: `2e03be154979430d4408443ad54b303da3251a14`, re-measured at
`11ab615136a0944e7b28f433cc0cfc46f207b092`

### Description
`_walk_body` renders the authority's `PLACEMENT_CONNECTOR_MIXING` verdict **before** the
admissibility guard, and only the two RETURN-PATH reasons are guarded:

```python
if verdict is not None:
    reason, at, message = verdict
    if reason == PLACEMENT_CONNECTOR_MIXING:
        raise raise_compile_error(PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY, ..., _join(path, *at), ...)
    # The return-path reasons are guarded by admissibility, the generic slot check is not.
    if is_allowed(context, TERMINAL_SLOT, "process_call"):
        raise raise_compile_error(PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED, ...)
```

The parser side never asks the authority at all in those contexts:
`DecisionFalseArmV1._arm_rules`, `TryCatchTryBodyV1` and `TryCatchCatchBodyV1` do not call
`_check_process_call_terminal_form` — only `BranchLegV1` and `DecisionTrueArmV1` do. So for a
body whose terminal slot does NOT admit a `process_call` but whose step slot DOES admit a
`connector_call`, the parser refuses via the discriminated-union rejection and points at
`/terminal`, while the compiler renders the mixing verdict and points at the connector's step
index. The comment three lines below states the intended rule — "Where the slot never
admitted the kind, the slot check is the true diagnosis and must win" — and the mixing branch
is the one place it is not applied.

The affected contexts are exactly `decision_false_arm`, `try_body` and `catch_body` — the
three of the five in `PROCESS_CALL_PLACEMENT_CONTEXT_LABELS` whose terminal set excludes
`process_call` while their step set includes `connector_call`.

### Steps to Reproduce
```python
# PATH A — live authoring boundary
build_integration(profile="renera", action="plan", config=json.dumps({"authoring_request": {
  "contract_version": "2", "intent": {"intent_kind": "process_ir", "integration_name": "x",
   "components": [{"key": "child", "type": "process", "action": "create",
                   "name": "_QA_FIXTURE_noop", "component_id": "31bb57cd-…",
                   "config": {"reference_only": True}},
                  {"key": "op", "type": "connector-action", "action": "create",
                   "name": "_QA_FIXTURE_op", "component_id": "2273565d-…",
                   "config": {"reference_only": True}}],
   "units": [{"envelope": {"component_key": "root", "name": "…", "action": "create",
                           "folder_name": "Training-Hlib-Bochkarov",
                           "depends_on": ["child", "op"]},
              "process_ir": {"version": "1", "body": {"kind": "sequence", "steps": [
                {"kind": "decision", "comparison": "equals",
                 "left":  {"value_type": "static", "static_value": "a"},
                 "right": {"value_type": "static", "static_value": "b"},
                 "true_arm":  {"steps": [SET], "terminal": {"kind": "stop"}},
                 "false_arm": {"steps": [{"kind": "connector_call", "operation_ref": "$ref:op"}],
                               "terminal": {"kind": "process_call", "process_ref": "$ref:child"}}}]}}}]}}}))

# PATH B — mutated exported model, straight to the compiler
ir = parse_process_ir_v1(<the same document with a LEGAL false arm>)
ir.body.steps[0].false_arm.steps    = [<a validated connector_call node>]
ir.body.steps[0].false_arm.terminal = [<a validated process_call node>]
compile_process_ir_v1(ir, SymbolTableV1())     # or validate_body_capabilities(ir)
```

### Expected / Actual
Expected: one document, one diagnosis. Per the module's own stated rule the slot check wins
where the slot never admitted the kind, i.e. `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY`
at `…/false_arm/terminal` on BOTH paths.

Actual (verbatim, this run):

```
C11  A=[('PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY', '/steps/0/false_arm/terminal')]
     B=[('PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY', '/steps/0/false_arm/steps/0')]
T1   A=[… '/steps/0/try_body/terminal']      B=[… '/steps/0/try_body/steps/0']
T2   A=[… '/steps/0/try_body/terminal']      B=[… '/steps/0/try_body/steps/0']
T4   A=[… '/steps/0/catch_body/terminal']    B=[… '/steps/0/catch_body/steps/0']
```

Direct confirmation through the real compile entry point (not just its delegate):
```
C11  compile_process_ir_v1 -> [('PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY',
                                '/body/steps/0/false_arm/steps/0')]
```

The CODE agrees in all four; only the pointer differs. Both paths refuse, so nothing illegal
is accepted.

### Additional Context
Root cause is a half-adopted authority: the compiler renders the verdict for all five body
contexts, the parser invokes it for two. Moving the `PLACEMENT_CONNECTOR_MIXING` raise
inside the same `is_allowed(context, TERMINAL_SLOT, "process_call")` guard that already wraps
the return-path reasons makes all four arms agree, and is consistent with the comment already
in the file. (Note the guard is only correct for a body whose CALL is the terminal — a call in
a STEP slot alongside a connector, T3, already agrees and must keep doing so.)

---

## QA-175-r4pa-02: the ROOT-sequence process-call rule is still two independent copies — the two entry points disagree on the pointer, and on the CODE when a connector is present

**Kind**: live-tool defect
**Severity**: Medium
**Blocking class**: machine-served schemas/contracts
**Tool/Area**: `build_integration` (plan, `intent_kind="process_ir"`) ·
`src/boomi_mcp/compiler/process_ir/body_capabilities.py` `_check_process_call_placement` ·
the root `SequenceBodyV1` validators in `src/boomi_mcp/models/process_ir.py`
**Affected SHA**: `2e03be154979430d4408443ad54b303da3251a14`, re-measured at
`11ab615136a0944e7b28f433cc0cfc46f207b092`

### Description
`process_call_placement_verdict` is deliberately BODY-local and is not consulted for the root
sequence. The root rule therefore still exists as two hand-written copies — the parser's root
validators and the compiler's `_check_process_call_placement` — and they disagree in the same
way the body copies used to:

1. **Pointer disagreement (R1, R2, R3).** The parser points at the OTHER step (the node that
   should not be beside the call); the compiler points at the CALL itself. For root
   `[process_call, stop]` the caller is told `/body/steps/1` by one path and `/body/steps/0`
   by the other, for the same code.
2. **Code disagreement (R4, R5).** With a connector at the root beside a call, the parser
   serves `PROCESS_IR_CAPABILITY_UNSUPPORTED` at `/body` (no node-level pointer at all), while
   the compiler serves `PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED` at
   the offending step index. A machine consumer branching on the code takes a different branch
   depending on which entry point it used.

### Steps to Reproduce
Same two paths as `QA-175-r4pa-01`, with these root bodies (no control node required):
`{"kind": "sequence", "steps": [PC, STOP]}`, `[SET, PC]`, `[PC, SET]`, `[CONN, PC]`, `[PC, CONN]`.
Path B grafts the same node list onto a parsed legal `{"kind": "sequence", "steps": [PC]}`.

### Expected / Actual
Expected: one document, one code, one pointer.

Actual (verbatim, this run):

```
R1  A=[('…PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED', '/steps/1')]  B=[(same code, '/steps/0')]
R2  A=[('…PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED', '/steps/0')]  B=[(same code, '/steps/1')]
R3  A=[('…PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED', '/steps/1')]  B=[(same code, '/steps/0')]
R4  A=[('PROCESS_IR_CAPABILITY_UNSUPPORTED', '/body')]                 B=[('…RETURN_PATH_BINDING_UNSUPPORTED', '/steps/1')]
R5  A=[('PROCESS_IR_CAPABILITY_UNSUPPORTED', '/body')]                 B=[('…RETURN_PATH_BINDING_UNSUPPORTED', '/steps/0')]
```

Direct confirmation through the real compile entry point:
```
R1  compile_process_ir_v1 -> [('PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED', '/body/steps/0')]
R4  compile_process_ir_v1 -> [('PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED', '/body/steps/1')]
```

Both paths refuse every one of R1–R5; the legal singleton root `[process_call]` is accepted
by both (S2 control P3). Note this is not a regression introduced by this round's commit —
the r3 measurement recorded the parser serving `/body/steps/1` for `[pc, pc, stop]`, which is
the same "point at the other step" behaviour. What is new is that the round's stated
invariant ("one authority, both entry points render it") does not extend to the root slot,
and the sweep shows the untouched copy pair still disagreeing.

### Additional Context
The authority's own docstring scopes itself out of this: *"This check is BODY-LOCAL and not
sufficient alone."* That is accurate, but it leaves the root pair unpinned. The narrow fix is
to give the root its own single authority the same way the bodies got one; the alternative is
to make the root a body context and reuse `process_call_placement_verdict`. Deciding which
belongs to the author, not to QA.

---

## S3 — `dlq.mode="error_subprocess_ref"` + `catch_exception` — PASS

New instrument: `.claude/agent-memory/boomi-qa-tester/harness/175-dlq-catch-plan-gate.py`.

**A probe correction worth recording**: the first version of this probe put the `reliability`
block on a `wrapper_subprocess` and measured plan-accepts / apply-creates, which looks exactly
like a P0 regression. It is not.
`WrapperSubprocessBuilder.validate_config` never calls `_validate_reliability`, so the block is
silently ignored on a wrapper and the composition is never examined. The gate lives on
`database_to_api_sync`, which is what r1 originally exercised. The result below is from the
corrected probe. (That a wrapper silently accepts an unsupported `reliability` key is
pre-existing behaviour outside this slice's diff — recorded as an observation, not filed.)

Fixture provenance: the config is the SERVED
`get_schema_template(resource_type="process", operation="create",
protocol="database_to_api_sync").example_component_spec` (its `config` keys printed in the
log: `description, folder_name, process_extensions, process_kind, reliability, source,
target, transform`), with `$ref` placeholders replaced by ids read live out of the account.

The spec is two components — a valid `wrapper_subprocess` `first`, then the offending sync
process `second` depending on it. That ordering is load-bearing: r1 measured the
single-component apply leaving the inventory flat anyway; the real mutation only appeared once
another component was ordered ahead of the refusal.

```
PLAN [first=wrapper, second=sync dlq=error_subprocess_ref + catch_exception]
   _success=True  execution_order=['first', 'second']
   steps: [["first",  "create",                    null,                          null],
           ["second", "error_process_validation",  "PROCESS_CALL_CONFIG_INVALID", "reliability.catch_exception"]]
   -> PASS

APPLY (dry_run=False) of the SAME refused composition
   _success=False  error=Plan contains unresolvable steps. No operations were executed.
   results: {}
   inventory 24 -> 24 -> 24
   -> PASS (ZERO mutation)

NON-VACUITY CONTROL: the SAME two-component spec with a legal composition
   (dlq.mode=error_subprocess_ref, NO catch_exception)
   _success=True  results={"first": {"status":"created","component_id":"f684452a-…"},
                           "second": {"status":"created","component_id":"bb5d0363-…"}}
   inventory 24 -> 26  (delta +2)
   -> PASS (the probe CAN detect a mutation)
```

Teardown deleted both control components (filtered to `status == "created"`); inventory
returned to 24.

### S3b — served remediation for the dedicated code — PASS, with one recorded nuance

Served verbatim with
`PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED`:

> Author the process call as the TERMINAL of its path and remove whatever followed it — a call
> whose child returns no documents ends the path, so a trailing stop is not needed and cannot
> be emitted. A call that must hand control onward needs its child's return-document shapes
> bound to it; that capability is published as **process_call_return_path_binding** at
> `get_schema_template(schema_name='process_ir_authoring', category='capability')`.

- names the gated capability: **yes**
- routes the caller to the published capability entry: **yes**
- carries the literal string `#176`: **no** — and neither does the capability contract entry
  (`'#176' in capability payload: False`). Served diagnostics in this contract are static,
  value-free strings and carry no issue numbers anywhere; `#176` appears only in source
  comments. Recorded, not filed as a defect: the machine-facing half of "points at #176" is
  the capability pointer, and that is present.

---

## S5 — the corrected `PROCESS_CALL_ORPHAN_CONTINUATION` message (commit `11ab615`) — PASS

New instrument: `.claude/agent-memory/boomi-qa-tester/harness/175-orphan-message-claim-grade.py`.
The mid-run commit rewrote served message text, which is machine-facing contract, so its
claim is graded by construction rather than read. Fixture provenance: every shape form is
copied byte-for-byte from the m11 capture (`start` shape1, `branch` shape17, `processcall`
shape10, `stop` shape2); only dragpoint targets and `numBranches` are retargeted.

Served verbatim now:

> Process Call 'shape10' declares no return path from the called process but carries an
> outgoing connection. The called process's Return Documents shapes are what make a forward
> connection valid, so the platform does not bind this connection and it does not exist in
> the emitted graph.

- **Control** — an edge-less `shape99` in the same graph IS reported `SHAPE_UNREACHABLE`, so
  the absence assertions below are non-vacuous: PASS.
- Orphan code still fires on the call: PASS.
- The dragpoint's target (`shape2`) is NOT reported unreachable: PASS.
- The message no longer asserts unreachability, and carries the bounded claim: PASS.

**The commit's rationale is confirmed and is in fact stronger than stated.** The rationale
names the sibling-leg case; measured here, the verifier's reachability walk FOLLOWS the
call's dragpoint, so the target is reported reachable even when that dragpoint is its ONLY
inbound edge (arm B: `unreachable: []`). The old wording therefore contradicted the
verifier's own output in every case, not just the sibling-leg one.

S5 failures: 0.

---

## Worktree hygiene

`git status --porcelain` in the shared repo: EMPTY (verbatim: no output).
`git grep -n "MUT-" -- 'src/**'`: no output (rc=1).
No mutation controls were needed this round — every divergence was measured directly at the
two live entry points, so no worktree copy was created and no file under
`/Users/gleb/Documents/Projects/Renera/boomi-mcp-server` was modified. Writes were confined
to `agents/reports/` and `.claude/agent-memory/boomi-qa-tester/harness/`.

Verbatim, at the end of the run:

```
$ git rev-parse HEAD
11ab615136a0944e7b28f433cc0cfc46f207b092
$ git status --porcelain
$ git grep -n "MUT-" -- 'src/**'
rc=1
```

## Instruments harvested

- `.claude/agent-memory/boomi-qa-tester/harness/175-placement-authority-agreement.py`
- `.claude/agent-memory/boomi-qa-tester/harness/175-placement-authority-siblings.py`
- `.claude/agent-memory/boomi-qa-tester/harness/175-dlq-catch-plan-gate.py`
- `.claude/agent-memory/boomi-qa-tester/harness/175-orphan-message-claim-grade.py`
