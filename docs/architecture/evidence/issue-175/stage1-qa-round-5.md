# QA round 5 — issue #175 — served KB text, compatibility rows, placement precedence

**Status: COMPLETE — CLEAN on every dispatched scenario.** Two findings, both non-blocking
in effect (r5-01 already fixed by the main thread mid-round; r5-02 Low, served wording).

## Baseline attestation

At dispatch:

```
$ git rev-parse HEAD
5bad33bd409e8feeab3e619f22b814d51f3b83b4

$ git status --porcelain
(empty)

$ git log --oneline -1
5bad33b Discharge the round-4 gate findings and defer the parity class to 178
```

**The shared tree moved TWICE during this run, both times by the main thread, never by me.**

1. At ~15:50 `tests/fixtures/m12_12/legacy_reachability_inventory.json` appeared as ` M`
   in the shared worktree (the wave gate's own rebaseline). The freeze guard caught it as
   `tree=dirty:db59f1320bc0` at the S3 PRE stamp.
2. The main thread then committed it:

```
$ git rev-parse HEAD
63a8ec71bfd1b20fc4db0b64c5ababbedf5e6774
63a8ec7 Rebaseline the served-surface digests for the corrected doctrine text
5bad33b Discharge the round-4 gate findings and defer the parity class to 178

$ git status --porcelain
(empty)

$ git diff --stat 5bad33b 63a8ec7
 .../m12_12/legacy_reachability_inventory.json | 294 ++++++++++-----------
 1 file changed, 147 insertions(+), 147 deletions(-)
```

**No `src/` or `server.py` byte changed between the two commits**, and the tree-freeze
guard's loaded-code fingerprint is **identical across both** — `code=5b39bc9f4b26` at
`5bad33b` (agreement bar, siblings bar, S1, S1b, S3 first run) and `code=5b39bc9f4b26` at
`63a8ec7` (S3 final run). Every result below is therefore attributable to one and the same
loaded code. Scenarios were re-run at `63a8ec7` where noted.

I modified **no** tracked file. My only writes were this report (gitignored),
`/private/tmp/.../scratchpad/`, and a detached worktree at `/tmp/qa175r5-mut` (removed).

## Scenario ledger

| # | Scenario | Attested SHA | Verdict |
|---|---|---|---|
| S1 | Served `wrapper_subprocess_separation` doctrine text | `5bad33b` | **PASS** (6/6) |
| S1b | Withdrawn-form sweep, 120 served surfaces / 16,365 string leaves | `5bad33b` | **PASS** (0 genuine hits, 6/6 rules non-vacuous) |
| S2 | Served schema/capability surfaces vs the inventory rows | `5bad33b` | **PASS** with 1 Low finding (r5-02) |
| S2-live | Inventory `executable` row graded on the emitted XML | `63a8ec7` | **PASS** (4/4) |
| S3 | Placement precedence, 22 arms, both entry points | `63a8ec7` | **PASS** (22/22, 0 peer mismatches) |
| S4 | `PROCESS_CALL_ORPHAN_CONTINUATION` served message | `63a8ec7` | **PASS** (5/5) |
| S5 | Accept/refuse ASYMMETRY sweep | `5bad33b`+`63a8ec7` | **PASS** (0 / 46 documents) |
| R | r4pa regression: both r4pa findings | `5bad33b` | **CLOSED** (24/24 agree) |

---

## S1 — the served doctrine entry (PASS)

Retrieved through both public KB surfaces; the catalog and single-pattern payloads are
byte-identical for this entry.

- `get_schema_template(schema_name="design_doctrine")` → `_success=True`, `entry_count=39`
- `get_schema_template(schema_name="design_pattern:wrapper_subprocess_separation")` →
  `design_pattern.boomi_shape_mapping` identical to the catalog entry

Served `boomi_shape_mapping`, verbatim tail (the delta):

> … A subprocess hands its documents back to the calling parent through a Return Documents
> terminal at the end of its document path — the subprocess return value, emittable today by
> the typed builder and live-verified — which Process Route maps to named return paths; a
> Return Documents path never routes onward to a Stop. **On the CALLER side that hand-back is
> not yet expressible: Boomi projects a Process Call's outbound connection from the called
> process's Return Documents shapes, and binding them is gated as
> process_call_return_path_binding (#175/#176). Today a Process Call is a path TERMINAL — a
> parent calls its child and ends there, with no trailing Stop and no outgoing connection.**

All six assertions PASS: retains the child-side `emittable today` claim (#107); names the
gate `process_call_return_path_binding`; states the call is a path TERMINAL; states the
caller-side hand-back is not yet expressible; cites #175/#176; states no trailing Stop.
`capability_status` remains `emittable_today` and `provenance` `live_verified` — correct,
since the CHILD-side terminal is what the entry certifies as emittable.

## S1b — withdrawn-form sweep across every served surface (PASS)

**Method correction, recorded because it changes what the zero means.** My first sweep
searched for the *removed wording* and reported 0 hits — but its non-vacuity control did
not fire, because the fix was **additive**: the child-side sentence was retained and
qualified, not deleted. A zero from that sweep proved nothing. The rewritten sweep hunts the
withdrawn **claim** (that execution continues past a Process Call) and proves each rule
non-vacuous twice — against a hand-written canary and against the same canary injected into a
real served payload and re-swept by the same code.

- Universe: the machine-served `valid_schema_names` enumeration (**105** names, not a hand
  list) + `list_capabilities` + `list_integration_archetypes` + every archetype page +
  `plan_integration_design` + `search_boomi_docs` → **120 surfaces, 16,365 string leaves**
- 6 rules; **6/6 fire on their canary**; **6/6 find the injected canary end-to-end**
- **16 raw hits, all false positives** — every one is the *new correct* wording:
  - ×12 `BranchLegV1.description` / `DecisionTrueArmV1.description` across 6 schema surfaces:
    "A `process_call` is a TERMINAL, never a step, and admits no step prefix: a call ends the
    path it is on, because whether execution continues past it is determined by the called
    process's return-…" (matched on "continues")
  - ×3 "Boomi projects a Process Call's **outbound connection** from the called process's
    Return Documents shapes, and binding them is gated" (the doctrine entry, its
    single-pattern twin, and `notes[6]` of the wrapper schema)
  - ×1 "the wrapper emits **start -> Process Call and stops there**"

**No other served doctrine or KB entry advertises the withdrawn form.**

## S2 — served schema/capability surfaces vs the inventory rows (PASS, 1 Low finding)

`get_schema_template(resource_type="process", protocol="wrapper_subprocess")` agrees with
both rewritten rows:

- `summary`: "EXACTLY ONE call, and the call ENDS the process - no trailing Stop"
- `supported_terminal_shapes`: `["processcall"]`
- `field_notes.process_calls`: "EXACTLY ONE entry (#175) … More than one entry returns
  `PROCESS_CALL_CONFIG_INVALID` on field `process_calls`."
- `field_notes.return_documents`: "GATED (#175) … `enabled=true` returns
  `PROCESS_CALL_CONFIG_INVALID` on field `return_documents.enabled`"
- `notes[6]` states the #175 rule and both newly-refused shapes

Targeted contradiction probes over the full JSON — `return_documents` offered for
wrapper_subprocess, "ordered"/"chain" `process_calls`, "start → processcall(s) → stop" — all
**clean**. See QA-175-r5-02 for the one residue.

## S2-live — the inventory rows graded on the LIVE emitted XML (PASS, 4/4)

A row about what is *executable* is a claim about emitted XML, so it was graded by applying
real wrappers to the live account and reading the stored components back
(`invoke_boomi_api GET /Component/<id> accept="xml"`). Account 24 → 26 → 24; both created
components deleted in `finally` (filtered to this run's `_TEST_QA175R5` names).

**Acceptance row — the two newly-refused shapes, graded at PLAN with exact code AND field:**

| arm | config | served code | served field | served message | inventory |
|---|---|---|---|---|---|
| W2 | two `process_calls` entries | `PROCESS_CALL_CONFIG_INVALID` | `process_calls` | "wrapper_subprocess supports exactly one process call, got 2." | 24 → 24 |
| W3 | `return_documents.enabled=true` | `PROCESS_CALL_CONFIG_INVALID` | `return_documents.enabled` | "wrapper_subprocess cannot return documents after its process call." | 24 → 24 |

**Executable row — applied, then read back from the platform.** Identical for W1 (one call)
and W4 (one call + `return_documents.enabled=false`), which is the positive control proving
the retained optional field is a genuine no-op:

```
shapes: [{"name":"shape1","type":"start","dragpoints":["shape2"]},
         {"name":"shape2","type":"processcall","userlabel":"c0","dragpoints":[]}]

<shape image="processcall_icon" name="shape2" shapetype="processcall" userlabel="c0"
       x="256.0" y="96.0"><configuration><processcall abort="false"
       processId="31bb57cd-d018-42cc-acf4-bf8cc624d916" wait="true">
       <parameters/><returnpaths/></processcall></configuration><dragpoints/></shape>
```

All seven assertions PASS on both: exactly one `start`, exactly one `processcall`, shape
count 2, **no `stop` shape**, **no `returndocuments` shape**, the `processcall` carries
**no dragpoint** (`<dragpoints/>` empty), and the start's dragpoint targets it. Empty
`<returnpaths/>` + zero dragpoints is exactly the terminal form of the four UI-built calls in
the m11 captures — the independent oracle frozen 2026-07-03.

The inventory's `executable` row (`start → processcall`, the call terminating the path, no
trailing stop or return) is therefore confirmed against the live platform, not merely against
the builder. This is also the measured basis for QA-175-r5-02: the emitted graph provably has
nothing after the call.

## S3 — placement precedence (PASS, 22/22)

Every arm run through **both** public entry points:
**PATH A** = live `build_integration(action="plan")`, `intent_kind="process_ir"`;
**PATH B** = a mutated exported model (`ProcessIRV1` is exported and not frozen) handed to
the public `validate_body_capabilities` **and** to the full `compile_process_ir_v1` — the
root-yield arms are exactly the ones whose refusal must come from a later stage, so the body
checker alone would be a vacuous probe. `plan` moved the inventory by **0** (24 → 24).

### 3a — the root control-continuation yield (G1–G12, all PASS)

`process_call_root_verdict` now returns `None` when a control node precedes the last step.
That yield is sound: **every** such document is still refused, and refused **identically** to
the ordinary-kind peer.

| arm | root | PATH A | PATH B (compile) |
|---|---|---|---|
| G1 | `[branch, process_call]` | `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` @ `/body/steps/0` |
| G2 | `[branch, set_dpp]` | *identical to G1* | *identical to G1* |
| G3/G4 | `[decision, process_call]` / `[decision, set_dpp]` | `…CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `…AMBIGUOUS_FLOW` @ `/body/steps/0` |
| G5/G6 | `[try_catch, process_call]` / `[try_catch, set_dpp]` | `…CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` @ `/body/steps/0` |
| G7/G8 | `[branch, process_call, stop]` / `[branch, set_dpp, stop]` | `…CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `…AMBIGUOUS_FLOW` @ `/body/steps/0` |
| G9/G10 | `[set_dpp, branch, process_call]` / `…, set_dpp]` | `…CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `…AMBIGUOUS_FLOW` @ `/body/steps/0` |
| G11 | `[process_call, branch]` — call FIRST | `…PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED` @ `/body/steps/1` | same |
| G12 | `[connector_call, branch, process_call]` | `…CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `…AMBIGUOUS_FLOW` @ `/body/steps/0` |

**Peer comparison: 5/5 pairs identical on both paths** (code AND pointer). The dispatch's
claim — a `process_call` after a control node is refused *the same way* as an ordinary kind —
holds exactly. G11 confirms the yield is correctly scoped: with the call FIRST the verdict
does **not** yield and the dedicated code is still served.

### 3b — a non-admitting slot under an ancestor connector (K1–K6, all PASS)

This is the guard the delta added (`and is_allowed(context, TERMINAL_SLOT, "process_call")`
on the cross-nesting mixing branch). Both entry points agree on code AND pointer in all six.

| arm | document | both paths |
|---|---|---|
| K1 | `[connector_call, decision(FALSE.terminal=process_call)]` | `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` @ `…/steps/1/false_arm/terminal` |
| K2 | `[connector_call, decision(FALSE.steps=[connector_call], terminal=process_call)]` | same code, same pointer |
| K3 | `[connector_call, try_catch(connector-scope, CATCH.terminal=process_call)]` | `…NODE_NOT_ALLOWED_IN_BODY` @ `…/steps/1/catch_body/terminal` |
| K4 | `[connector_call, try_catch(connector-scope, TRY.terminal=process_call)]` | `…NODE_NOT_ALLOWED_IN_BODY` @ `…/steps/1/try_body/terminal` |
| **K5** | **CONTROL** `[connector_call, decision(TRUE.terminal=process_call)]` | `…NODE_NOT_ALLOWED_IN_BODY` @ `…/steps/1/true_arm/terminal` |
| **K6** | **CONTROL** `[connector_call, branch(leg0.terminal=process_call)]` | `…NODE_NOT_ALLOWED_IN_BODY` @ `…/steps/1/legs/0/terminal` |

K5/K6 are the **non-vacuity controls for the new guard**: where the slot *does* admit a call,
the ancestor mixing branch still fires. So the guard narrowed the branch to non-admitting
slots rather than disabling the cross-nesting rule.

### 3c — the legal forms (L1–L4, all PASS)

Graded through the **full compiler**, with a symbol table that actually resolves `$ref:child`
(derived at run time, not assumed — an unresolvable table is how a harness manufactures a
fake asymmetry, and it did in my first pass).

- **L1** root `[decision(TRUE.terminal=process_call)]` — **ACCEPTED** by A, `validate_body`, and `compile`
- **L2** root `[branch(leg0.terminal=process_call)]` — **ACCEPTED** by all three
- **L3** root `[process_call]` singleton — **ACCEPTED** by all three
- **L4** root `[decision(TRUE.steps=[set_dpp], terminal=process_call)]` — a step **prefix**
  before a terminal call: refused by design on both paths,
  `…PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED` @ `…/steps/0/true_arm/terminal`

No P0-class regression: every legal form still works.

## S4 — the `PROCESS_CALL_ORPHAN_CONTINUATION` message (PASS, 5/5)

Standing bar `harness/175-orphan-message-claim-grade.py`, re-run at `63a8ec7`.
Served message, verbatim:

> Process Call 'shape10' declares no return path from the called process but carries an
> outgoing connection. The called process's Return Documents shapes are what make a forward
> connection valid, so the platform does not bind this connection and it does not exist in
> the emitted graph.

- the rule still **fires** on an empty `<returnpaths/>` beside an outgoing dragpoint — PASS
- the message no longer asserts the target shapes are "left unreachable" — PASS
- it carries the bounded claim (the connection is not bound / not in the emitted graph) — PASS
- **non-vacuity control**: an edge-less `shape99` in the same graph IS reported
  `SHAPE_UNREACHABLE`, so "shape2 not reported unreachable" is a real measurement — PASS
- arm B re-measures the underlying fact: the verifier's reachability walk FOLLOWS the call's
  dragpoint, so the old wording contradicted the verifier's own output in *every* case

## S5 — accept/refuse asymmetry (PASS, 0 asymmetries)

The dispatch asked me to probe deliberately for a document ACCEPTED by one entry point and
REFUSED by the other (not covered by the #178 deferral). Across **46 documents** — 22 S3 arms,
14 agreement-bar arms, 10 sibling-sweep arms — **zero**. Every document is accepted by both
paths or refused by both.

My first S3 pass reported 3 "asymmetries" (L1/L2/L3). Those were **my harness**: PATH B
compiled with an empty `SymbolTableV1()`, so `$ref:child` could not resolve and the compiler
correctly returned `PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND`. With a resolving symbol table
all three are accepted by both paths. Recorded because a fake top-severity finding is worse
than none.

## R — r4pa regression check: both findings CLOSED

| bar | at `11ab615` (r4pa) | at `5bad33b` |
|---|---|---|
| `harness/175-placement-authority-agreement.py` (14 arms) | r4pa-01 divergence | **14/14 agree**, 3 positive controls accepted |
| `harness/175-placement-authority-siblings.py` (10 arms) | **8/10 diverged** | **0/10 diverge** |

Both entry points now agree on code AND pointer for try_body, catch_body, decision_false_arm
and all five illegal ROOT shapes — including the two that previously disagreed on the CODE
(`[connector_call, process_call]` and `[process_call, connector_call]` now both serve
`PROCESS_IR_CAPABILITY_UNSUPPORTED` @ `/body` on both paths).

---

## Findings

## QA-175-r5-01: served-surface digest fixture was stale at `5bad33b` (fixed at `63a8ec7`)
**Kind**: live-tool defect (derived-artifact staleness)
**Severity**: Medium
**Blocking class**: machine-served schemas/contracts
**Tool/Area**: `tests/fixtures/m12_12/legacy_reachability_inventory.json` / `src/boomi_mcp/kb/design_doctrine.py`
**Affected SHA**: `5bad33bd409e8feeab3e619f22b814d51f3b83b4` — **resolved at `63a8ec71bfd1`**

### Description
The delta edited served KB text (`design_doctrine.py`) and a served docstring
(`process_flow_builder.py`) without rebaselining the derived served-surface digest fixture,
so the m12_12 reachability/served-surface guard was RED at the committed tip.

### Steps to Reproduce
In an **isolated** worktree at the tip (so no shared-tree state can be blamed):

```
git worktree add --detach /tmp/qa175r5-mut 5bad33bd409e
cd /tmp/qa175r5-mut
PYTHONPATH=src:. .venv/bin/python tests/_m12_12_legacy_inventory.py --check ; echo "EXIT=$?"
```

### Expected / Actual
Expected `EXIT=0`. Actual:

```
changed served artifacts (3):
  - SS-SCHEMA-TEMPLATES:schema_name=design_doctrine
  - SS-SCHEMA-TEMPLATES:schema_name=design_pattern:wrapper_subprocess_separation
  - SS-SCHEMA-TEMPLATES:walked_surface_digest
EXIT=1
```

### Additional Context
A full regeneration (`--write` to a scratch path) differs from the committed fixture in
**147 leaves**: 106 `census[].evidence_line`, 32 `ledger_rows`, 9 `served_artifacts`. The
`evidence_line` shifts come from the one line the delta added to the `process_flow_builder.py`
module docstring, which moved `from .process_emitters.legacy import (` from line 79 to 80.

Two observations worth carrying:

1. **`--check` under-reports relative to `--write`.** It flagged 3 served artifacts; the full
   regeneration differs in 147 leaves. If the census/ledger drift is deliberately outside
   `--check`'s remit that is fine, but the two are not interchangeable and only `--write`
   sees the whole drift.
2. The wave gate running in the main thread caught this independently and it was committed as
   `63a8ec7 "Rebaseline the served-surface digests for the corrected doctrine text"` while
   this round was in flight. Recorded for the ledger, not as open work.

---

## QA-175-r5-02: two served `field_notes` still describe the parent CONTINUING past the call
**Kind**: live-tool defect (served contract text)
**Severity**: Low
**Blocking class**: machine-served schemas/contracts
**Tool/Area**: `get_schema_template(resource_type="process", protocol="wrapper_subprocess")` → `field_notes` (`src/boomi_mcp/categories/meta_tools.py:8465-8466`)
**Affected SHA**: `5bad33b` / `63a8ec7` (both)

### Description
The same served payload that states "the call ENDS the process - no trailing Stop" also
carries two field notes describing a continuation after the call. They are **pre-existing and
unchanged by this delta** (identical at the pre-slice baseline `3fd5027e`), but the delta is
what makes them contradict the surface they sit in — and the rewritten inventory row now
says the call TERMINATES the path.

### Steps to Reproduce
```python
r = get_schema_template(resource_type="process", protocol="wrapper_subprocess")
r["field_notes"]["process_calls[].wait"]
r["field_notes"]["process_calls[].abort_on_error"]
```

### Expected / Actual
Expected: wording consistent with `summary` ("the call ENDS the process"). Actual, verbatim:

```
"process_calls[].wait":
    "Wait for the child to finish before continuing (default true)."
"process_calls[].abort_on_error":
    "Abort the parent if the child fails (default false — the parent continues,
     matching the live wrapper exemplar)."
```

### Additional Context
This is a precision defect in machine-facing text, not a behavioural one — the behaviour is
graded separately in S2-live below and the emitted graph has no post-call shape. The
defensible reading of `abort_on_error` is "the parent does not fail" and of `wait` is "the
parent proceeds (to its end)", but a machine consumer choosing between these fields reads
"continues" as flow continuation, which #175 just declared impossible. The same phrasing
family also appears in the `process_flow_builder.py` module docstring ("it continues past a
child failure") — that one I did **not** confirm as served.

This is served contract text, so it is blocking-class by policy; I record it Low because the
enclosing payload states the correct rule three times (`summary`, `supported_terminal_shapes`,
`notes[6]`) and no behaviour depends on it.

Measured basis (S2-live, not a reading): the applied wrapper's stored XML contains exactly
two shapes with the `processcall` carrying `<dragpoints/>` empty, so there is provably no
shape for the parent to "continue" to. The emitted attributes are `abort="false" wait="true"`,
matching the notes' stated defaults — only the *consequence* they describe is wrong.

---

## Shared-worktree hygiene (dispatch requirement)

Verified at the end of the round:

```
$ git rev-parse HEAD
63a8ec71bfd1b20fc4db0b64c5ababbedf5e6774

$ git status --porcelain
(empty)

$ git grep -n "MUT-" -- 'src/**'
(no output, rc=1)
```

The detached worktree `/tmp/qa175r5-mut` used for QA-175-r5-01 was removed and pruned. I made
no mutation control this round — every arm was reachable through the public tool boundary or
through the exported-model entry point, so no source edit was needed.

## Instruments harvested

- `harness/175-placement-precedence-matrix.py` — the 22-arm precedence matrix (G/K/L),
  both entry points, peer comparison, asymmetry check, run-time-derived symbol table
- `harness/175-withdrawn-form-sweep.py` — the 6-rule withdrawn-form sweep over the served
  `valid_schema_names` universe, with canary + seeded end-to-end non-vacuity controls
- `harness/175-wrapper-inventory-rows-live.py` — the live apply + XML-readback grading of the
  compatibility-inventory acceptance and executable rows
- `harness/175-served-doctrine-entry.py` — the doctrine-entry retrieval and assertion bar

---

## Consolidated note for issue #178 (diagnostic-identity divergences — do NOT re-file)

Every divergence below has **both** entry points refusing; none is an accept/refuse split.
New instances observed this round, on top of the corpus already enumerated in #178:

| document | PATH A (`parse_process_ir_v1`) | PATH B (`compile_process_ir_v1`) |
|---|---|---|
| root `[branch, process_call]` (and `[branch, set_dpp]`) | `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` @ `/body/steps/0` |
| root `[decision, X]`, `[branch, X, stop]`, `[set_dpp, branch, X]`, `[connector_call, branch, X]` | same as above | same as above |
| root `[try_catch, X]` | `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` @ `/body` | `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` @ `/body/steps/0` |
| root `[connector_call, try_catch(process-scope, …terminal=process_call)]` | `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` @ `…/catch_body/terminal` | `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` @ `/body/steps/0` |

The pattern is consistent: the divergences are **not** `process_call`-specific — the ordinary-kind
peer diverges identically, which is evidence they belong to the general
control-continuation/scope layer rather than to #175.
