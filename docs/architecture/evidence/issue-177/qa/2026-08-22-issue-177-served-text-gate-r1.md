# QA — issue #177 (M12, served-text ↔ enforcement consistency gate) — Stage 1, round 1

**Dispatch baseline SHA**: `6f26caff7481356119fee5b36a1730cec0fb5df2` (tree DIRTY — the slice is uncommitted)
**Mode**: live-local (`BOOMI_LOCAL=true`), profile `renera` → `traininghlibbochkarov-JKIY2X`
**Freeze**: every scenario ran inside `harness/tree-freeze-guard.py`. `import_race=NO`,
`code_stable=True`, `worktree_moved=False` on all nine probes. Stamps
`6f26caff7481/dirty:b7c141f1f601/{3ebc0b0b434f | 1d3f65bb2a87 | 84848d980c0e | 7027117c61ab | f1ab1e5e2905}`
(the `code=` hashes differ only by which lazily-imported modules each probe pinned; the
worktree fingerprint `dirty:b7c141f1f601` is identical on every probe, so all results are
attributable to one and the same tree).
**Suite**: NOT re-run in full (dispatch instruction). Named node sets only — 292 tests across the
five affected test files, used as the mutation-grading bar.

**Verdict: every dispatched scenario PASSES. 2 findings, both Low and both non-blocking prose
(internal docstrings/comments, proven not served). No blocking finding.**

---

## S1 — `get_schema_template(schema_name='process_ir_authoring', category='diagnostic')`, whole category

Paged through the cursor: **3 pages, 57 entries** (20 + 20 + 17).

| assertion | result |
|---|---|
| entries with a blank `summary` | **0** |
| entries with a blank/empty remediation-equivalent (`ordering_facts`) | **0** (min `len(ordering_facts)` = 2) |
| blank string leaves ANYWHERE in the category | **0** |
| summaries that are a **registered message** | **57 / 57** |
| summaries that are a **remediation** | **0 / 57** |

**Non-vacuity** — both blank detectors were run against seeded payloads containing a `""` and a
`"   "` value and both fired (`['CTL_BLANK','CTL_WS']`, `['$.a.b','$.c[1]']`); a canary planted in a
real registry `_MESSAGES` entry reached the served page (2 hits) and disappeared on restore (0 hits).
So the zeroes above are measurements, not artefacts.

### The seven codes, served `summary` verbatim, and how each reads

| code | served summary (verbatim) | reads as |
|---|---|---|
| `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | `this node kind is not admitted in this control-body slot` | what is wrong |
| `PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID` | `compiler-derived control-node wiring is invalid` | what is wrong |
| `PROCESS_IR_SCHEMA_BRANCH_CARDINALITY` | `branch leg count is outside the 2-25 bound` | what is wrong |
| `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` | `continuation after a branch or decision is not supported` | what is wrong |
| `PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED` | `ProcessIR v1 emits no join: a node has more than one predecessor` | what is wrong |
| `PROCESS_IR_SEMANTIC_NESTING_LIMIT` | `control nesting exceeds the ProcessIR v1 depth bound` | what is wrong |
| `PROCESS_IR_SEMANTIC_UNTERMINATED_PATH` | `a divergent control path reaches no terminal` | what is wrong |

All seven read as "what is wrong". None is a remediation.

### A/B against the pre-slice tree (`git archive 6f26caf` into a sandbox, same probe, same fixture)

Exactly **three** summaries changed, and they are the three the dispatch named:

```
PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID
  base: 'This is a compiler defect: derived branch/decision wiring (count, order, labels, or
         target) is wrong. Please report it with the authored path.'      <- a REMEDIATION
  head: 'compiler-derived control-node wiring is invalid'

PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED
  base: 'Give each divergent path its own terminal. ProcessIR v1 emits no join or merge, so a
         node may have at most one predecessor.'                          <- a REMEDIATION
  head: 'ProcessIR v1 emits no join: a node has more than one predecessor'

PROCESS_IR_SEMANTIC_UNTERMINATED_PATH
  base: 'End every Branch leg and Decision outcome in its own terminal; each divergent path must
         terminate independently.'                                        <- a REMEDIATION
  head: 'a divergent control path reaches no terminal'
```

`ordering_facts` attribution also moved, correctly: three codes went `[parser] …` → `[compiler/parser] …`
(the compiler message is now byte-identical to the parser's, so the merge coalesces them), and
`PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` gained a distinct `[compiler] node placement or path
composition is not admitted in this control body` beside the parser's narrower sentence. Entry count
and code set unchanged (57 ↔ 57, no adds, no drops).

**Recorded correction to the dispatch's premise**: the projection never served a literally blank
`summary` at the baseline either — `_diagnostic_summary` fell back to the remediation. The observable
pre-slice defect was remediation-as-summary on three codes, not an empty field. The empty field was
real one layer down (`compiler_diagnostic_specs()` returned `message: ""`), which is what the
fail-closed change addresses.

---

## S2 — `category='capability'`

* **27 rows exactly**, single page.
* Served capability-id set == `PROCESS_IR_V1_CAPABILITIES` key set (0 missing, 0 extra).
* Per-row: served `source_state` is byte-equal to the manifest value for all 27; served
  `canonical_state` equals the projection's **own published** `state_mappings()` image of that value
  for all 27 (the expectation is read from the runtime mapping, never hand-typed). **0 mismatches.**
* Identical at the baseline tree — this category did not move in this slice.

---

## S3 — served revision digests at this tree (verbatim)

```
schema_revision      sha256:86c59e6ed37f4036cd9a06427d14b1809f1f3bb587be520d3b2255d69152e2c7   UNCHANGED
capability_revision  sha256:c08021bc5c4003d49b3637fd9395a7d4fdb3a04e8ca45cb27d2c2131eef6f14c   MOVED
compiler_revision    sha256:fe235787e4749ac90c45d4382e00890ac3ef6fffe596f7d238a407d1a7ec95b5   MOVED
schema_hash          sha256:dd101accb22210ef1e0a4721116f40648ffa4fc921693ab0d31d05c311bc0c36   UNCHANGED

baseline (6f26caf, pre-slice):
capability_revision  sha256:6c31d6948f595e8916d70c89b943a8daddbd55f33c1b5e375a2c7d4bbab66137
compiler_revision    sha256:559150b6efed83cf689a646fcbbb27c1f11bfedee723e71690a9a2dbcc70d370
```

Both moving digests are served **identically** by all four surfaces that carry them:
`list_capabilities().authoring_contract`, `get_schema_template(schema_name='process_ir_authoring')`,
and the `revision_binding` on both the diagnostic and capability category pages. `schema_revision`
and `schema_hash` correctly stand still (no JSON schema moved).

The checked-in golden `tests/fixtures/authoring_contract/process_ir_authoring_v1.contract.json` is
**in sync with what the tool serves**: 182 entries each side, id sets equal, **0 entries differing**,
`diagnostic_label_legend` equal.

---

## S4 — refusal documents, full served quadruples (`build_integration`, live)

Identical at `action="plan"` and `action="compile"` in every case.

**S4a — Branch with ONE leg**
```
code       : PROCESS_IR_SCHEMA_BRANCH_CARDINALITY
path       : /intent/units/0/process_ir/body/steps/0/legs
message    : branch leg count is outside the 2-25 bound
remediation: A Branch must declare between 2 and 25 legs (the platform's documented bound).
(envelope error_code INVALID_INPUT, "…failed ProcessIRV1 validation at 1 location(s).")
```

**S4b — valid two-leg Branch followed by a `message` step**
```
code       : PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED
path       : /intent/units/0/process_ir/body
message    : no step may follow a branch or decision — control nodes are terminal fan-out in
             ProcessIR v1 (continuation_after_branch_or_decision is gated)
remediation: Move the steps that followed the branch/decision into every leg or arm — ProcessIR v1
             emits no continuation after a control node.
```

**PARSER layer unchanged — proven, not assumed.** Both documents were run through the identical
probe on the pre-slice tree. A 22-case-arm differential (14 MCP-boundary arms × plan/compile, 8
compiler-entry arms) comparing every `(code, path, message, remediation)` quadruple returned
**0 differing cases**. The slice moves the served SPEC TABLE only; it changes no raised diagnostic.

The reason is structural and was verified at source: every raise site for the seven codes passes an
explicit `message=` to `diagnostic()` / `raise_compile_error()` / `invariants._fail()`, so the new
canonical `_MESSAGES` entries are only ever consulted by `compiler_diagnostic_specs()`.

---

## S5 — nested `try_catch`, and the call-site-vs-canonical precedence check

**KEY CHECK PASSES on both public entries.**

*MCP boundary* (`build_integration(action="compile")`, a `try_catch` inside another `try_catch`'s try body):
```
code       : PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY
path       : /intent/units/0/process_ir/body/steps/0/try_body/steps/0
message    : this node kind is not admitted in this control-body slot
remediation: Use a node kind this body slot admits. The admitted set for each slot is published at
             get_schema_template(schema_name='process_ir_authoring', category='placement'); a kind
             absent from a slot is rejected, so absence is the rule, not an omission.
```
The specific placement sentence wins. The new generic canonical message
(`node placement or path composition is not admitted in this control body`) does **not** appear —
it is reachable only through `ordering_facts` on the served contract entry, which is where it belongs.

*Compiler entry.* Because the MCP boundary refuses at the AuthoringRequestV1 intake parse, that arm
answers from the PARSER table, not from `body_capabilities._check`. To reach the compiler's own raise
site the probe used the documented second public entry — build a legal `ProcessIRV1`, mutate
`try_body.steps` after validation, hand it to `compile_process_ir_v1`:
```
PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY @ /body/steps/0/try_body/steps/0
  :: 'this node kind is not admitted in this control-body slot'
```
Same call-site message. Byte-identical at the baseline tree.

The related cross-nesting arm still carries its own distinct call-site sentence, confirming the
canonical registration did not flatten the family:
```
PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY @ /body/steps/0/legs/0/steps/0
  :: 'a process_call may not share a root-to-leaf path with a connector step — a connector runs
      upstream in this branch leg (step 0; process_call_connector_mixing is gated)'
```

---

## S6 — one ADMITTED capability compiled end-to-end

Fixture `tests/fixtures/process_ir/rich_control/branch_process_call.json` (provenance: committed
`3c07ad2`, 2026-08-20, pre-baseline). Structure used verbatim; the symbolic `child_process` ref bound
to logical key `$ref:child` → `_QA_FIXTURE_noop` (`31bb57cd-d018-42cc-acf4-bf8cc624d916`) declared
`reference_only` with `envelope.depends_on=["child"]`.

* `action="plan"` → `_success: true` (2 pre-existing `PROCESS_IR_SEMANTIC_LINEAGE_EFFECT_UNKNOWN`
  warnings on the subprocess terminals, unchanged from baseline)
* `action="compile"` → `_success: true`, `mutation_performed` absent/false
* Same fixture through `compile_process_ir_v1` directly → **COMPILED**

Identical at the pre-slice tree. The compile path is unbroken.

**Account hygiene**: this round performed **no apply, no create, no deploy, no execute**. Only
`plan`/`compile` and read-only reference tools were driven. `_QA_FIXTURE_noop` referenced read-only
and untouched; no `_TEST_` component was created, so nothing needed teardown.

---

## Additional grading the dispatch did not ask for, but the slice's own claims require

### Fail-closed behaviour, fault-injected, one case per process

The three accessors now raise instead of serving a blank. With intact registries that branch is
unreachable, so it was measured by perturbing ONE registry before the first call and driving the real
public tools. A0 is the paired positive control.

| case | `get_schema_template` diagnostic page | `list_capabilities` | live refusal | canary |
|---|---|---|---|---|
| **A0 control** (intact) | `_success:true`, 20 entries | full index, healthy digests | `PROCESS_IR_SCHEMA_BRANCH_CARDINALITY` | 0 |
| A1 parse: remediation w/o message | `_success:false`, 0 entries | `process_ir_authoring: {"status":"unavailable"}`, **both digests move** | unchanged | 0 |
| A3 parse: blank message on a real code | same | same | unchanged | 0 |
| A4 compiler: remediation w/o message | same | same | unchanged | 0 |
| A6 semantic: remediation w/o message | same | same | unchanged | 0 |

The served failure text is exactly:
`The 'process_ir_authoring' authoring contract cannot be built: an authority it derives from is unavailable (ValueError).`
— no code names, no traceback, no file path, no authored content. The canary planted in the
offending code name and in the offending remediation value reached **no** served leaf (sweep proven
non-vacuous by C1 above). An honest `unavailable` is what a caller gets, and the compile/refusal
runtime path is entirely unaffected.

**The deliberate asymmetry holds**: `diagnostic()` does NOT fail closed. For an unregistered code it
returns `message='compiler rejected the payload'` with the generic remediation; under a broken
registry it still returns the real static message. Neither raises.

### Guard mutation grading (a guard is code; an ungraded guard is not evidence)

Run in a sandbox copy of the working tree (`git archive 6f26caf` + the slice's own files,
`diff -r` proven byte-identical to the working tree's `src/`), against the five affected test files
(292 tests). Every mutant reintroduces a defect the slice claims to close.

| mutant | result |
|---|---|
| M0 / M13 control (no mutation) | 292 passed |
| M1 unregister `PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED` message | **KILLED** |
| M2 blank the new `CONTROL_WIRING` message | **KILLED** |
| M3 compiler message with no remediation (asymmetry) | **KILLED** |
| M4d blank the existing semantic-validation message | **KILLED** |
| M4e remove the semantic-validation message (asymmetry) | **KILLED** |
| M5 a compiler raise site for an **unregistered** code | **KILLED** (`test_every_emittable_process_ir_code_has_complete_served_text`, `test_the_served_code_set_is_exactly_what_the_authorities_account_for`) |
| M6 parse registry: remediation with no message | **KILLED** |
| M7b flip `"joins": "gated"` → `"supported"` | **KILLED** (`test_every_witness_kind_matches_the_live_manifest_state`, `test_a_capability_state_change_moves_compiler_revision`) |
| M8 remove ALL seven #177 registrations (full revert) | **KILLED** |
| M10b revert the remediation broadening to `branch/decision wiring` | **KILLED** (`test_the_whole_contract_is_frozen_in_a_committed_snapshot`) |
| M12 replace a #177 summary with a wrong-but-nonblank string | **KILLED** (same snapshot test) |

**8/8 real mutants killed, both controls green.** Two harness self-corrections are recorded because
they each produced a false survivor first: (1) rounds 1–2 anchored on `_MESSAGES = {` while
`findings.py` declares `_MESSAGES: Dict[str, str] = {`, so the mutant was never applied and reported
SURVIVED; (2) the first node set omitted the two `*_parity.py` files, which is where the byte-compared
contract golden lives, so M10 falsely survived. Both were caught by asserting the anchor and by
widening the set, not by reading the pass line.

**Observation (recorded, not filed)**: the two halves of the gate are pinned by different
mechanisms. Text *completeness* is pinned by the new derived guard, which fails on any registry that
is asymmetric or blank and on any raise site whose code is unregistered. Text *content* is pinned
only by `test_the_whole_contract_is_frozen_in_a_committed_snapshot` — a golden snapshot. That is a
legitimate bidirectional pin (a wording change fails until deliberately rebaselined), so it is not a
gap; it is worth knowing that a content regression surfaces as a snapshot diff rather than as a
named enforcement failure.

### Served-text ↔ enforcement accuracy of the broadened remediation

The broadening from "branch/decision wiring" to "control-node wiring" was verified against the
enforcement rather than taken on trust: `invariants._CONTROL_KINDS = frozenset({"branch",
"decision", "try_catch"})`, and `check_emission_plan_invariants` raises
`PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID` for `emitter_kind in {decision, branch, catcherrors}` —
four of the seven raise sites are the try/catch dragpoint-label and dragpoint-row checks. The old
text under-described what the compiler rejects; the new text is accurate.

### `2-25` literal vs its runtime authority

`BranchNodeV1.model_fields["legs"]` carries `min_length=2, max_length=25`; the served message reads
`branch leg count is outside the 2-25 bound` at **both** the parser and (new) compiler layers, so the
literal currently matches. It is a hand-copy rather than a derivation — pre-existing at the parser
layer, and the slice adds a second instance at the compiler layer with byte-identical text. Both are
covered by the frozen contract snapshot, so drift is detectable. Recorded as an observation, not
filed.

---

## QA-177-r1-01: `_diagnostic_summary`'s docstring describes behaviour the slice made impossible and cites a tombstoned test

**Kind**: live-tool defect (source prose)
**Severity**: Low
**Blocking class**: non-blocking (internal docstring — proven not served)
**Tool/Area**: `src/boomi_mcp/authoring/process_ir_projection.py` (`_diagnostic_summary`, lines ~1382–1415)
**Affected SHA**: `6f26caf` + uncommitted slice

### Description
The docstring is the projection's own written account of the fallback the slice set out to empty. At
this tree every one of its load-bearing statements is false:

* "**Seven** compiler codes have no entry in the static `_MESSAGES` table" — measured: **0** codes
  lack a message at any layer.
* "Only **THREE** reach this fallback: `PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID`,
  `PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED` and `PROCESS_IR_SEMANTIC_UNTERMINATED_PATH`" — measured:
  **0** codes reach it; all three now serve a registered message.
* "…for those three the only authored sentence available is the remediation — which makes their
  summary a 'how to fix' rather than a 'what is wrong'" — no longer true of any code.
* "every count in this docstring — the seven, the four and the three — is measured by
  `test_exactly_the_expected_codes_fall_back_to_their_remediation`" — that node is **tombstoned** in
  this same slice (`tests/fixtures/wave_gate/test_nodes.jsonl:4850`, `"state":"tombstone"`) and
  replaced by `test_no_compiler_diagnostic_falls_back_to_its_remediation`. The docstring's named
  measurement authority no longer exists, so the counts are now unmeasured prose asserted in the
  place the file explicitly says they must not be asserted.

The function body's fallback loop is correspondingly dead: `_complete_spec_rows` now guarantees a
non-blank message for every registered code at all three layers, so `row["message"]` can never be
empty.

### Steps to Reproduce
```
PYTHONPATH="src:$PWD" ./.venv/bin/python   # scratchpad/p8_consistency.py
# merges the three served spec tables and compares each served summary against them
C2 summaries that are a remediation: []
C2 summaries not a registered message: []
C3 codes with NO message at any layer: []
```
```
git grep -n "test_exactly_the_expected_codes_fall_back_to_their_remediation" -- tests/
# only two hits: the replacement test's own docstring, and a "state":"tombstone" ledger row
```

### Expected / Actual
**Expected**: the docstring describes the current behaviour, or is reduced to the property the
replacement test now proves.
**Actual**: it describes the pre-#177 behaviour in specific counted detail and cites a removed test
as its authority.

### Additional Context
Not blocking: a sweep of every served payload (405 KB across `list_capabilities`, the
`process_ir_authoring` root template, all ten category pages, and the 45 `mcp.list_tools()`
descriptions) finds **0** occurrences of `"Seven compiler codes have no entry in the static"` and
**0** of the tombstoned test name, while the same sweep finds two control phrases that ARE served
(`"branch leg count is outside the 2-25 bound"`, `"this node kind is not admitted in this
control-body slot"`) — so the sweep is non-vacuous and this text genuinely never reaches a caller.
`git grep '__doc__\|inspect.getdoc' -- src/` returns 0. Fits the one batched non-blocking pass.

---

## QA-177-r1-02: the sibling block comment in `findings.py` still documents the blank-serving behaviour the slice removed

**Kind**: live-tool defect (source prose)
**Severity**: Low
**Blocking class**: non-blocking (internal comment — proven not served)
**Tool/Area**: `src/boomi_mcp/compiler/process_ir/semantic_validation/findings.py`, lines ~210–216
**Affected SHA**: `6f26caf` + uncommitted slice

### Description
The identical three-place comment "A code carrying one of the two texts but not the other is emitted
with an empty string rather than skipped: a caller comparing the served set against the codes they
actually receive has to be able to see the gap" existed above all three accessors. The slice
corrected it in `diagnostics.py` (now: "Since #177 a code carrying one of the two texts but not the
other cannot exist…") and removed it in `models/process_ir.py` (replaced by `_complete_spec_rows`).
The `findings.py` copy is unchanged and sits **directly above** `finding_specs()`, whose own
docstring two lines later says "Fails closed since #177". The file therefore states both the old and
the new contract, three lines apart, with the stale statement first.

This is the second-instance shape the repo's structural-fix rule targets: one fact hand-copied to
three sites, two updated and one missed. It is prose only, so it is a sibling-sweep miss rather than
a defect-class instance requiring an invariant.

### Steps to Reproduce
```
sed -n '208,228p' src/boomi_mcp/compiler/process_ir/semantic_validation/findings.py
```
```
#: … A code carrying one of the two texts but not the other
#: is emitted with an empty string rather than skipped: a caller comparing the
#: served set against the codes they actually receive has to be able to see the
#: gap.


def finding_specs() -> Tuple[Mapping[str, str], ...]:
    """Static (code, message, remediation) for every semantic-validation code.

    Fails closed since #177; see ``_complete_spec_rows`` …
```

### Expected / Actual
**Expected**: the comment matches `diagnostics.py`'s corrected sibling, or is deleted in favour of
the docstring below it.
**Actual**: it asserts the removed behaviour, immediately above the function that no longer has it.

### Additional Context
Verified not served by the same 405 KB sweep (0 hits for both `"is emitted with an empty string
rather than skipped"` and `"has to be able to see the"`, with the two positive controls firing).
Fold into the same non-blocking batch as QA-177-r1-01.

---

## Worktree at exit

`git rev-parse HEAD` = `6f26caff7481356119fee5b36a1730cec0fb5df2`; `git status --porcelain` is
**byte-identical to the dispatch state** (same 16 rows). No tracked file modified by this run, no
commit, no stash created (the two listed stashes pre-date this session), no worktree created — the
baseline and sandbox trees were produced with `git archive` + `cp` into the session scratchpad and
the sandbox `src/` was re-verified byte-identical to the working tree after the last mutant. All
probe artefacts live in the session scratchpad.
