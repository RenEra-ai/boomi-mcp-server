# QA — issue #177 (M12, served-text ↔ enforcement consistency gate) — Stage 1, round 2

**Tree under test**: `371c533a52dea293f7e0d57117f51b133ebd661e` ("Read forwarded defaults at runtime
and reach the enforcement in every witness"), `git status --porcelain` **EMPTY** at dispatch and at
exit.
**Pre-correction comparison tree**: `7aa79b3154abb19da200fbddfd61169ed47c97a5` — the committed
QA-validated baseline, i.e. the tree round 1 measured. Verified: `diff -r` over `src/` between
`7aa79b3` and the working tree reports exactly **one** differing file,
`src/boomi_mcp/authoring/process_ir_projection.py`. The dispatch's premise that the production delta
is that one file is therefore measured, not assumed.
**Mode**: live-local (`BOOMI_LOCAL=true`), profile `renera` → `traininghlibbochkarov-JKIY2X`.
**Freeze**: every scenario ran inside `harness/tree-freeze-guard.py`. `import_race=NO`,
`code_stable=True`, `worktree_moved=False` on all probes. The two trees carry **different loaded-code
fingerprints** (`code=16a799e4e43d` at HEAD vs `code=b3ab701d75de` in the pre-correction sandbox),
which is what makes an identical served result a measurement rather than an accident of loading the
same bytes twice.
**Suite**: full non-KB suite NOT run (dispatch instruction). Named node set only — 6 affected test
files, **553 tests** (551 executed, 2 deselected, see the node-set note below).

**Verdict: all four dispatched checks PASS. No served-text divergence. One finding —
QA-177-r2-01, Medium, the new guard has zero test coverage.**

---

## S1 — no served regression (dispatch item 1)

`get_schema_template(schema_name='process_ir_authoring', category=…)` driven through the public tool
boundary, paged through its cursor.

| measurement | r1 recorded | r2 at `371c533` | verdict |
|---|---|---|---|
| diagnostic entries / pages | 57 / 3 | **57 / 3** | unchanged |
| blank summaries · blank `ordering_facts` · blank string leaves anywhere | 0 · 0 · 0 | **0 · 0 · 0** | unchanged |
| capability rows, id set == `PROCESS_IR_V1_CAPABILITIES`, state mismatches | 27 · equal · 0 | **27 · equal · 0** | unchanged |
| `schema_revision` | `sha256:86c59e6e…9152e2c7` | **identical** | unchanged |
| `capability_revision` | `sha256:c08021bc…f6f14c` | **identical** | unchanged |
| `compiler_revision` | `sha256:fe235787…7ec95b5` | **identical** | unchanged |
| `schema_hash` | `sha256:dd101acc…11bc0c36` | **identical** | unchanged |

All seven #177 summaries are byte-identical to the r1 verbatim capture (`compiler-derived
control-node wiring is invalid`, `ProcessIR v1 emits no join: a node has more than one predecessor`,
`a divergent control path reaches no terminal`, `branch leg count is outside the 2-25 bound`,
`continuation after a branch or decision is not supported`, `control nesting exceeds the ProcessIR v1
depth bound`, `this node kind is not admitted in this control-body slot`).

### Confirmed independently at the tool boundary, as asked — with a whole-payload digest

r1 persisted only the seven codes verbatim, so "the digest is unchanged" would have rested on a
subset. A new probe (`177r2-payload-digest.py`) dumps **every** entry with **every** field from both
categories, hashes the canonical JSON, and was run against both trees:

```
                          pre-correction 7aa79b3            HEAD 371c533
diagnostic  (57 entries)  sha256:5bbcbd729a08c04e93bd…  ==  sha256:5bbcbd729a08c04e93bd…
capability  (27 entries)  sha256:a6c93b48fff0dcf0c9d3…  ==  sha256:a6c93b48fff0dcf0c9d3…
root template             sha256:5ed8c7d3cd7f0c7b544c…  ==  sha256:5ed8c7d3cd7f0c7b544c…
revision_binding          byte-identical (all 4 digests, all 4 surfaces that carry them)
```

**Byte-identical served payload across the correction delta**, on a probe that reads the whole page
rather than a chosen subset, with the loaded-code fingerprints proving two genuinely different trees.

**Committed golden re-verified independently**: `tests/fixtures/authoring_contract/
process_ir_authoring_v1.contract.json` vs what `build_process_ir_authoring_entries()` serves —
**182 entries each side, id sets equal, 0 entries differing**, `diagnostic_label_legend` equal to
`DIAGNOSTIC_LABEL_LEGEND`. Non-vacuity control: one served entry's `summary` seeded with a
divergence → the comparator reported exactly 1 (control PASSES).

---

## S2 — the new refusal is honest and leaks nothing (dispatch item 2)

Fault injected **one case per process** (the `list_capabilities` manifest is memoized — the r1 trap).
Every case perturbs one row of one spec tuple of an injected `ProjectionSourcesV1` and plants a
canary in the row's *other* authored field, so a leak has something distinctive to be found by.

| case | perturbation | raised | served / observed |
|---|---|---|---|
| **B0 control** | none (production snapshot verbatim) | — | 182 entries, 57 diagnostic, 0 blank, 0 canary |
| B1 | compiler `message = ""` | **ValueError** | `compiler diagnostic source serves a blank message for PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED` |
| B2 | compiler `message = "   "` | **ValueError** | same sentence (whitespace-only is caught) |
| B3 | compiler `remediation = ""` | **ValueError** | `…serves a blank remediation for …` |
| B4 | **parse** `message = ""` | **ValueError** | `parser diagnostic source serves a blank message for PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` |
| B5 | **semantic** `message = ""` | **ValueError** | `semantic validator diagnostic source serves a blank message for LEGACY_ADAPTER_EXEMPTION_DECISION_PROPERTY_READ` |
| B6 | compiler `message = None` | **ValueError** | same sentence |
| B7 | compiler `message = 12345` | AttributeError | `'int' object has no attribute 'strip'` — see Additional Context of QA-177-r2-01 |

**All three producers fire**, not only the compiler. For every ValueError case: the message names the
`stage_label`, the field name and the **CODE**, and nothing else — measured, not read:
`message_mentions_code = True`, `message_mentions_canary = False`,
`message_mentions_old_value = False`, `message_mentions_path_or_module = False` (the last checks for
`/`, `\`, `.py`, `boomi_mcp`, `Traceback`, `line `).

### The layer above still degrades, not 500s

Case **B8**: `collect_projection_sources()` patched to return the B1-perturbed snapshot, then the
**real public MCP tools** driven.

```
get_schema_template(…, category='diagnostic'):
  {"_success": false,
   "error": "The 'process_ir_authoring' authoring contract cannot be built: an authority it
             derives from is unavailable (ValueError).",
   "error_code": "AUTHORING_SCHEMA_SOURCE_UNAVAILABLE",
   "retryable": true, "schema_name": "process_ir_authoring", "status": "unavailable"}

list_capabilities().authoring_contract.process_ir_authoring:  {"status": "unavailable"}
```

Identical for the root template. **Zero uncaught exceptions** at any of the four surfaces — no 500.
Byte-identical to the r1 served failure text.

**Leak sweep over the whole served blob**: `canary_remediation_hits 0`, `canary_summary_hits 0`,
`code_name_hits 0`, `traceback_hits 0`, `module_path_hits 0`. (`py_file_hits 54` is `list_capabilities`'s
own pre-existing SDK coverage lists — `"query_packaged_components.py"` etc. — inspected in context and
unrelated to the fault.)

**Non-vacuity of the leak sweep (case B9, the positive control the negative needs)**: the identical
probe with a *valid, non-blank* perturbation carrying the same two canaries → the build SUCCEEDS and
the sweep finds `canary_remediation_hits 3`, `canary_summary_hits 1`, `code_name_hits 4`. The zeros
in B8 are measurements, not a blind sweep.

**Case B10** (non-string value at the serving layer): also degrades honestly —
`…unavailable (AttributeError).`, `{"status": "unavailable"}`, 0 leaks, 0 uncaught. So B7's
AttributeError is caught by the same `except Exception` handler and never reaches a caller as a crash.

---

## S3 — live refusals unaffected (dispatch item 3)

The full r1 differential re-run on both trees, live, through `build_integration`:

**ARM A (MCP boundary, 14 arms = 7 cases × plan/compile): 0 differing.**
**ARM B (compiler entry, `compile_process_ir_v1` on post-validation-mutated models, 8 arms): 0 differing.**
**TOTAL: 0 of 22 arms differ** on any of `(code, path, message, remediation)`, `_success`, or
`mutation_performed`. Non-vacuity on both trees: arm B reports `{'compiled': 1, 'refused': 7}`, arm A
2 successes and 12 refusals — the battery can tell accept from refuse.

**The two dispatched quadruples, verbatim at HEAD, identical at `plan` and `compile`:**

*1-leg Branch* — matches r1 byte for byte:
```
code       : PROCESS_IR_SCHEMA_BRANCH_CARDINALITY
path       : /intent/units/0/process_ir/body/steps/0/legs
message    : branch leg count is outside the 2-25 bound
remediation: A Branch must declare between 2 and 25 legs (the platform's documented bound).
(envelope error_code INVALID_INPUT, "…failed ProcessIRV1 validation at 1 location(s).")
```

*Nested `try_catch`* — matches r1 byte for byte, and **still serves the call-site sentence**:
```
code       : PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
path       : /intent/units/0/process_ir/body/steps/0/try_body/steps/0
message    : this node kind is not admitted in this control-body slot
remediation: Use a node kind this body slot admits. The admitted set for each slot is published at
             get_schema_template(schema_name='process_ir_authoring', category='placement'); a kind
             absent from a slot is rejected, so absence is the rule, not an omission.
```
Compiler entry (arm B) serves the same call-site message at `/body/steps/0/try_body/steps/0`.

**The canonical registration does not leak into any refusal** — measured over the whole 22-arm
capture: `node placement or path composition is not admitted in this control body` **0 hits**,
`compiler-derived control-node wiring is invalid` **0 hits**, `ProcessIR v1 emits no join` **0 hits**,
against a control of `this node kind is not admitted in this control-body slot` = **6 hits**.

**Text-enforcement census at HEAD** (same instrument as r1, all readings unchanged): 0 summaries that
are a remediation, 0 summaries that are not a registered message, 0 served codes with no registry row,
0 codes with no message at any layer, registered union 57 == served 57, and the `2-25` literal still
matches `BranchNodeV1.legs` `min_length=2/max_length=25`. Canary control PASSES (2 hits seeded → 0
after restore).

**Account hygiene**: no apply, no create, no deploy, no execute. Only `plan`/`compile` and read-only
tools. `_QA_FIXTURE_noop` referenced read-only and untouched; nothing to tear down.

---

## S4 — non-vacuity of the new guard (dispatch item 4)

### Behavioural non-vacuity: PROVEN (the guard does fire, and it changes the outcome)

The pre-correction tree is the definitive mutant. Same probe, same fixtures, `7aa79b3` vs HEAD:

| case | HEAD `371c533` | pre-correction `7aa79b3` |
|---|---|---|
| B1 compiler `message=""` | **ValueError** | **no raise** — served summary = `"Use a connector family/action pair from the verified connector-call capability matrix… QA177R2CANARYREMEDIATION"` (the remediation, canary and all: 2 canary hits in the served entries) |
| B4 parse `message=""` | **ValueError** | no raise — the merge picked up another layer's message, so the blank was *invisible* |
| B5 semantic `message=""` | **ValueError** | no raise — served summary = `"No change required on the legacy surface… QA177R2CANARYREMEDIATION"` |
| B0 control | 182 entries | 182 entries |

Mutant-took-effect proof (per the r1 trap): the guard sentence occurs **1** time in HEAD's
`process_ir_projection.py` and **0** times in the pre-correction file. The correction closes exactly
the bypass it claims to close, on all three producers, and B4 shows it also catches a case the old
fallback masked entirely.

### Test-level non-vacuity: **FAILS** — see QA-177-r2-01

Five surgical mutants of the guard, graded against the 6 affected test files. Every injector asserts
its anchor and printed `applied <name>`; both controls green.

| mutant | result |
|---|---|
| N0 control (pristine HEAD) | 551 passed |
| **N1 delete the guard entirely (all 7 lines)** | **SURVIVED** — 551 passed |
| **N2 check `message` only, drop `remediation`** | **SURVIVED** — 551 passed |
| **N3 `if spec.get(field) is None:` (accepts `""` and `"   "`)** | **SURVIVED** — 551 passed |
| **N4 raise with the whole authored row interpolated (`row={3!r}`)** | **SURVIVED** — 551 passed |
| N5 revert the `_diagnostic_summary` docstring correction | SURVIVED — 551 passed (prose; expected) |
| N13 control again (pristine restored) | 551 passed |

**0/5 killed.**

**Node-set note.** Per the r1 trap, a survivor is a statement about the node set — so the set was
checked rather than assumed. Only **two** test files in the entire suite reference
`build_process_ir_authoring_entries` (`test_process_ir_authoring_contract.py`,
`test_process_ir_authoring_contract_parity.py`) and only those two reference `ProjectionSourcesV1` /
`collect_projection_sources`; **both are in the node set**, and neither file contains a single
`pytest.raises(ValueError)`. A grep of all of `tests/` for `serves a blank` / `blank message` /
`blank remediation` returns nothing relevant. The gap is real at any node set, not an artefact of
this one.

**Harness note (recorded so it is not rediscovered).** The first grading run had a RED control:
`tests/test_wave_gate.py::test_audit_ledger_revisions_are_append_only_and_fully_declared` and
`::test_audit_ledger_attestations_have_durable_matching_evidence` walk **git ancestry**, and a
`git archive` sandbox has no `.git`, so both fail identically under every mutant — the "+1 constant
failure" trap that launders every mutant into a false KILL. They are `--deselect`ed from the sandbox
runs and were confirmed **2 passed** in the real working tree instead.

---

## QA-177-r2-01: the new merge-level blank guard has zero test coverage — it can be deleted, halved, weakened or made leaky and the whole affected node set stays green

**Kind**: dark-slice/guard finding
**Severity**: Medium
**Blocking class**: non-blocking at this tree — no served divergence exists. Recorded against
*machine-served schemas/contracts* because that is the class of the property the guard defends: an
unwitnessed guard is a fail-open on a blocking-class invariant, and the repo's Structural-fix rule
requires a second-instance structural fix to ship "a non-vacuity witness (a test constructing a
concrete case the new invariant excludes)".
**Tool/Area**: `src/boomi_mcp/authoring/process_ir_projection.py`, `_diagnostic_entries`, lines
1145–1159
**Affected SHA**: `371c533a52dea293f7e0d57117f51b133ebd661e`

### Description

The correction adds a seven-line invariant to `_diagnostic_entries`:

```python
for field in ("message", "remediation"):
    if not (spec.get(field) or "").strip():
        raise ValueError(
            "{0} diagnostic source serves a blank {1} for {2}".format(
                stage_label, field, code
            )
        )
```

The behaviour is correct and was verified on all three producers (S2, S4). What is missing is any
test that fails when it is wrong. Four functionally distinct mutants — full deletion, dropping the
`remediation` half, accepting `""`/`"   "` by checking only `is None`, and interpolating the entire
authored spec row into the error text — all leave **551/551 named tests passing**, against a green
control.

This matters more than an ordinary coverage gap because of the guard's domain. `_complete_spec_rows`
in `models/process_ir.py` already refuses a blank *message* **and** a blank *remediation* for all
three production accessors, so **no production input can ever reach this guard**. Its entire reachable
domain is injected `ProjectionSourcesV1` snapshots — the drift-control mechanism that
`test_process_ir_authoring_contract_parity.py::_perturbed` exists for. A guard whose only inputs come
from tests, with no test exercising it, is by construction dead code at this tree, and a later
"remove unused branch" cleanup has nothing to stop it.

Two things this does **not** mean, both measured rather than reasoned:

* It is **not** a secrets/security finding. The N4 leaky-guard mutant was run through the full
  serving path (S2's B8 probe on an N4-mutated tree): the canaries reached **0** served leaves,
  because `contract.py` serves `type(exc).__name__` and never the exception message. The "names
  CODES only" property is defence-in-depth; the real no-leak boundary is one layer up and it holds.
* It is **not** a served-text divergence. S1 and S3 are byte-identical across the delta.

### Steps to Reproduce

```bash
# 1. Confirm the guard is behaviourally live (it is):
#    inject a ProjectionSourcesV1 with one blanked compiler message
QA177_HARNESS=<harness> PYTHONPATH="src:$PWD" ./.venv/bin/python 177r2-injected-fault.py B1 out.json
#  -> ValueError('compiler diagnostic source serves a blank message for
#                 PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED')

# 2. Delete the seven-line guard in a sandbox copy of 371c533 and run the affected node set:
pytest tests/test_process_ir_served_text_enforcement.py \
       tests/test_process_ir_capability_enforcement.py \
       tests/test_process_ir_authoring_contract.py \
       tests/test_process_ir_authoring_contract_parity.py \
       tests/test_process_ir_entrypoint_diagnostic_parity.py \
       tests/test_wave_gate.py \
  --deselect tests/test_wave_gate.py::test_audit_ledger_revisions_are_append_only_and_fully_declared \
  --deselect tests/test_wave_gate.py::test_audit_ledger_attestations_have_durable_matching_evidence
#  -> 551 passed, 2 deselected     (identical to the unmutated control)

# 3. Confirm nothing anywhere else covers it:
grep -rln "build_process_ir_authoring_entries" tests/   # 2 files, both in the set above
grep -n  "pytest.raises(ValueError" <those two files>   # 0 hits
```

### Expected / Actual

**Expected**: the invariant added to close the injection bypass ships with a witness — a test that
injects a `ProjectionSourcesV1` with a blank `message` (and one with a blank `remediation`, and one
whitespace-only) and asserts `pytest.raises(ValueError)` whose message names the code and contains
neither field's authored text. `_perturbed(...)` in
`tests/test_process_ir_authoring_contract_parity.py` is the mechanism, already present and already
used by five sibling drift controls two lines away.

**Actual**: no such test exists. The guard is deletable in one edit with a green suite.

### Additional Context

Root cause is visible in the commit shape: `371c533` adds 30 lines to `process_ir_projection.py` and
~300 lines of tests, but every added test targets the §8 capability-table doc parser (`_parse_capability_states`
heading/row selection), not the merge guard. The guard was added in the same batch as an unrelated
test hardening and inherited none of it.

**Second, minor, same guard**: `(spec.get(field) or "").strip()` raises `AttributeError:
'int' object has no attribute 'strip'` for a non-string value (case B7) rather than the honest named
`ValueError`. The outcome is still fail-closed and correctly served — case B10 drove it through the
public tools and got `…unavailable (AttributeError).`, `{"status": "unavailable"}`, 0 leaks, 0
uncaught — and a non-string can only arrive by injection, since the accessors are typed
`Mapping[str, str]`. Worth folding into the same edit (`str(spec.get(field) or "").strip()`) rather
than filing separately.

**Recorded, not filed**: the correction makes `_complete_spec_rows` the *second* hand-written
"no blank message or remediation" check rather than deriving one. The two are at genuinely different
layers with disjoint domains (accessor vs merge, production vs injected), so this reads as
defence-in-depth rather than a duplicated enumeration under the Structural-fix rule.

---

## Round-1 findings: both discharged (verified, not assumed)

* **QA-177-r1-01** (stale `_diagnostic_summary` docstring citing a tombstoned test) — **discharged**.
  The rewritten docstring states the two limits explicitly, cites the live
  `test_no_compiler_diagnostic_falls_back_to_its_remediation`, and moves the seven/four/three counts
  into an explicitly-labelled *History* paragraph that correctly describes
  `test_exactly_the_expected_codes_fall_back_to_their_remediation` as tombstoned. Its new claim about
  "the native process-graph verifier" was checked and is accurate:
  `categories/components/process_graph_verifier.py::verify_process_graph` is real and serves its own
  `(code, message, remediation)` outside the three registries.
* **QA-177-r1-02** (stale sibling comment in `findings.py`) — **discharged**. `grep -rn "is emitted
  with an empty string rather than skipped" src/` returns 0; the block now reads "Since #177 a code
  carrying one of the two texts but not the other cannot be served at all…".

---

## Worktree at exit

`git rev-parse HEAD` = `371c533a52dea293f7e0d57117f51b133ebd661e`; `git status --porcelain
--untracked-files=normal` **EMPTY**; no commit, no stash created (the 2 listed stashes and the 2
prunable worktrees pre-date this session — none was created by this run); `git grep "QA177" --
'src/**'` empty (rc=1). Every sandbox was produced with `git archive` + `cp` into the session
scratchpad; the mutation sandbox was restored to pristine and re-graded green (N13) before exit.
Instruments harvested to `harness/`: `177r2-payload-digest.py`, `177r2-injected-fault.py`,
`177r2-golden-sync.py`, `177r2-guard-mutants.{py,sh}`.
