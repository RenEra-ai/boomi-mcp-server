# QA — issue #175 r6 — post-fix re-run of the served wrapper field notes

**Status of this file:** COMPLETE. Round finished before this was written; nothing is still in flight.

**Verdict: CLEAN — 0 findings.** All four dispatched checks completed, none FAILED. Two
non-blocking notes are recorded at the end (neither is a finding; neither opens work).

Written at the dispatcher's explicit request for a durable on-disk record. My standing rule is
"clean run ⇒ no report file"; this file exists because two earlier rounds of this slice died
without producing anything. There are therefore **no `QA-175-r6-NN` finding IDs** — a clean round
mints none.

---

## Baseline attestation (verbatim)

```
$ git rev-parse HEAD
adb905a7cda227251936e67ef8599f500cb60ce9

$ git status --porcelain
(no output — EMPTY)
```

Tip commit message: `Archive QA rounds durably and correct the deferral records` — matches the
dispatch.

**The delta under test.** `git diff 63a8ec7 HEAD -- src/ server.py` is exactly **2 lines**, both in
`src/boomi_mcp/categories/meta_tools.py` (lines 8465-8466) — the two `wrapper_subprocess` field
notes. Everything else in `63a8ec7..adb905a` is docs, evidence archives, a regenerated inventory
fixture, and a comment-only addition to `tests/test_wave_gate.py`.

**Freeze guard** (`harness/tree-freeze-guard.py`) wrapped every scenario. Identical on all of them:

```
freeze: adb905a7cda2/clean/49b8a95907a8
[freeze] ATTEST ...: code_stable=True worktree_moved=False
[freeze] POST head=adb905a7cda2 code=49b8a95907a8 files=133 import_race=NO
```

No edit raced any import; the loaded-code fingerprint never moved. No round-voiding event.

---

## Scope → outcome

| # | Dispatched check | Outcome |
|---|---|---|
| 1 | The corrected `wait` / `abort_on_error` field notes; zero occurrences of the old phrasings, with a non-vacuity control | **PASS** |
| 2 | Whole-payload self-consistency of the served `wrapper_subprocess` template | **PASS** |
| 3 | Sibling served-surface sweep for the same claim shape | **PASS** |
| 4 | No regression: served doctrine entry + `build_integration` terminal emission | **PASS** |
| extra | The NEW `abort_on_error=false` claim, graded LIVE (never measured before) | **PASS — claim upheld** |
| extra | Entry-point accept/refuse symmetry spot-check on wrapper/terminal forms | **PASS — 0/10 divergences** |

---

## 1. The corrected field notes — PASS (the load-bearing half)

**Call:**
```python
get_schema_template(resource_type="process", protocol="wrapper_subprocess")
```
(Signature derived at run time: `get_schema_template(resource_type, operation, standard,
component_type, protocol, schema_name, authoring_entry_id, node_kind, category, capability_id,
workflow_stage, after_entry_id, limit)`.)

**Actual served output, verbatim:**

> `process_calls[].wait`
> "Wait for the child to finish before **the parent's own run completes** (default true).
> **Nothing follows the call** — it ends the path (#175) — so this decides whether the parent
> waits, **not what executes next**."

> `process_calls[].abort_on_error`
> "Abort the parent if the child fails (default false — **the parent run finishes normally instead
> of failing**, matching the live wrapper exemplar). Since the call ends the path (#175), this
> decides the parent's **OUTCOME, not which shape runs after it**."

Both match what the dispatch asked for: `wait` is bounded by the parent's own run and states
nothing follows the call; `abort_on_error` decides the OUTCOME, not which shape runs next.

**A1 — zero occurrences of the withdrawn phrasings.** Scanning every string leaf of the entire
served payload:

```
before continuing        hits=0 []
the parent continues     hits=0 []
```

Plus an exact-sentence assertion: neither withdrawn note appears verbatim anywhere in the payload.

**Provenance of the strings being hunted.** They are **not hand-typed**. The probe runs
`git show 63a8ec7:src/boomi_mcp/categories/meta_tools.py` and recovers the two prior note values by
walking the module AST for the `process_calls[].wait` / `process_calls[].abort_on_error` keys. The
probe therefore cannot be tuned to the new text, and it recovered:

```
process_calls[].wait            Wait for the child to finish before continuing (default true).
process_calls[].abort_on_error  Abort the parent if the child fails (default false — the parent
                                continues, matching the live wrapper exemplar).
```

**Non-vacuity — three independent controls, all through the SAME scanning routine.**

- **C1 (present-string control):** `EXACTLY ONE entry` → 1 hit; `ends the path` → 5 hits
  (`.field_notes.process_calls`, `…[].wait`, `…[].abort_on_error`, `.field_notes.return_documents`);
  `PROCESS_CALL_CONFIG_INVALID` → 4 hits. The scan demonstrably finds strings that are present.
- **C2 (regression control):** each withdrawn note re-injected into a deep copy of the **real**
  served payload at its **real** leaf → both **DETECTED**. A regression would be caught.
- **C3 (rule control):** all 6 sweep rules fire on a canary seeded into the real payload at a real
  leaf.

**A2 — corrected semantics present, 8/8:**

```
[PASS] wait: waiting is bounded by the PARENT'S OWN RUN      match="parent's own run"
[PASS] wait: states nothing follows the call                 match='Nothing follows the call'
[PASS] wait: names the call as ending the path               match='ends the path'
[PASS] wait: does NOT frame the flag as choosing what runs next  match='not what executes next'
[PASS] abort_on_error: names the parent's OUTCOME            match="parent's OUTCOME"
[PASS] abort_on_error: spells out the false branch           match='finishes normally'
[PASS] abort_on_error: names the call as ending the path     match='ends the path'
[PASS] abort_on_error: denies it selects a successor shape   match='not which shape runs after it'
```

---

## 2. Whole-payload self-consistency — PASS

68 string leaves swept across `summary`, `field_notes`, `structured_errors`,
`supported_terminal_shapes`, `notes`, `example_component_spec`, `example_child_note`.
`supported_terminal_shapes == ["processcall"]`.

**4 raw regex hits, all adjudicated as CORRECT text** (a regex hit is not a defect — the correct
explanation of terminality necessarily contains the phrase "whether execution continues past a
call…", where the grammatical subject is the *child*):

1. `.summary` (R1/R2) — "EXACTLY ONE call, and the call ENDS the process - no trailing Stop,
   because whether execution continues past a call is decided by the called process, which hands
   control back only through the return-document steps it declares (issue #175)." This is the
   retained-and-qualified **child-side** claim already validated at r5. Its force is that
   continuation is *not the caller's to author*. Correct.
2. `.notes[6]` (R4) — matched "…`PROCESS_CALL_CONFIG_INVALID` **before** anything is created: more
   than one entry in process_calls, and **return_documents**.enabled=true". This is the *refusal*
   enumeration. Correct.
3. `.notes[6]` (R5) — "Boomi **projects** a call's outbound connection from the CALLED process's
   return-document shapes, so a call whose child returns nothing has no outgoing connection at
   all". States the connection is platform-projected, not caller-authorable. Correct.

**No statement implies a shape executes after the Process Call, and none implies more than one call
is accepted.** Rule R3 ("more than one call accepted") returned zero hits here and across the whole
corpus.

---

## 3. Sibling served-surface sweep — PASS

**Universe (machine-derived, never a hand list):** every name in the served `valid_schema_names`
refusal enumeration (**105**), the three `resource_type`/`protocol` process templates,
`list_capabilities`, `list_integration_archetypes` + every archetype page,
`plan_integration_design`, `search_boomi_docs`, **and all 45 registered MCP tool descriptions from
`server.mcp.list_tools()`** — literally what an MCP client receives on `tools/list`.
**122 surfaces + 45 tool descriptions = 16,411 string leaves.**

**25 hits, all adjudicated benign:**

- ×12 `BranchLegV1.description` / `DecisionTrueArmV1.description` (replicated across the 6 schema
  surfaces that embed the same `$defs`) — "A `process_call` is a TERMINAL, never a step, and admits
  no step prefix: a call ends the path it is on, because whether execution continues past it is
  determined by the called process's return-document shapes rather than by this document.
  **Authoring anything after a call is rejected.**"
- ×6 `ProcessCallNodeV1.description` — "**Invokes another process, and TERMINATES the path it is
  on.** … ProcessIR v1 supports the non-returning form, where the call is the end of its path — so
  a call is authored as a TERMINAL, with nothing after it and no trailing stop. **Authoring a node
  after a call is rejected.**"
- ×2 doctrine (`design_doctrine`, `design_pattern:wrapper_subprocess_separation`) — "…**Today a
  Process Call is a path TERMINAL** — a parent calls its child and ends there, with no trailing
  Stop and no outgoing connection."
- ×4 the wrapper template itself (the four adjudicated in §2).
- ×1 **unrelated**: `boomi_account_info` tool description — "continue using it for subsequent
  calls" (about reusing an account id across tool calls; matched only via the generic `calls?`
  alternative).

**No other served surface implies a continuation past the call.**

### Method correction — why QA-175-r5-02 escaped r5's own sweep

This is the substantive lesson of the round. The r5 bar (`175-withdrawn-form-sweep.py`) matched
string **values** only. The defect lived at

```
field_notes["process_calls[].wait"] = "Wait for the child to finish before continuing (default true)."
```

whose grammatical subject ("a process call") is in the **key**, not the value. Every value-only
rule was therefore *structurally* blind to it and returned a confident zero. **A sweep's universe is
not only which payloads it reads — it is which part of each leaf it looks at.** The new bar scans
`flattened-leaf-path :: value`, with `.`/`_`/`[]` flattened to spaces so a `[^.;]{0,N}` proximity
rule is not severed by path separators.

### Sibling SOURCE instances — found, and proven NOT served

A static repo grep found the identical withdrawn wording still in two places:

- `src/boomi_mcp/categories/components/builders/process_flow_builder.py:3247` — a code comment
- `src/boomi_mcp/categories/components/builders/process_flow_builder.py:4789` — the
  `WrapperSubprocessBuilder` class docstring

both reading *"the parent continues past a child failure"* (describing `abort="false"` on the live
exemplar).

**Neither is served.** Measured, not assumed:

- 0 hits across all 122 served payloads and all 45 MCP tool descriptions.
- `git grep -E '__doc__|inspect\.getdoc' -- src/` → **0 hits**. No runtime code path reads a
  docstring.

**The control matters here and initially failed.** My first reachability answer was *vacuous*: the
positive control — "Thin wrapper-parent", the first line of that very docstring — also returned 0,
which proves the corpus never contained the docstring, not that the wording is absent. The trap was
that `server.mcp.get_tools()` does not exist on this FastMCP, and filtering `dir(server)` by
`__module__.startswith("boomi_mcp")` yields an **empty** corpus because the tool callables'
`__module__` is `"server"`. I rebuilt the corpus from `mcp.list_tools()` and re-asked only once the
control fired (`'Two selectors: resource_type'` → found in `get_schema_template`).

Per CLAUDE.md, prose/comments/docstrings not served to callers are outside the blocking classes.
Recorded below as a non-blocking note, not filed.

---

## 4. Regression — PASS

**Served doctrine entry** (`harness/175-served-doctrine-entry.py`):
`S1 positive failures: 0 ; S1b stale hits: 0`. The `wrapper_subprocess_separation` entry is
unchanged and correct on both KB surfaces.

**Terminal emission — graded on the LIVE emitted XML, not the builder.** Three wrappers applied
through `build_integration(action="apply", dry_run=False)` and read back via
`invoke_boomi_api GET Component/<id> accept="xml"`:

```
[PASS] W_false  shapes=['start','processcall']  stops=0 call_dragpoints=0
       pc_attrs={'abort':'false', 'processId':'f79a3e90-…', 'wait':'true'}
[PASS] W_true   shapes=['start','processcall']  stops=0 call_dragpoints=0
       pc_attrs={'abort':'true',  'processId':'f79a3e90-…', 'wait':'true'}
[PASS] W_noop   shapes=['start','processcall']  stops=0 call_dragpoints=0
       pc_attrs={'abort':'false', 'processId':'31bb57cd-…', 'wait':'true'}
```

Exactly one `start`, exactly one `processcall`, **no `stop`**, **no outgoing dragpoint from the
call**, and the `abort` attribute correctly tracks `abort_on_error`.

**Acceptance rows** (`harness/175-wrapper-inventory-rows-live.py`): `W1 W2 W3 W4` all **PASS**,
`S2-live failures: 0`. Multi-call and `return_documents.enabled=true` still refuse at PLAN with
`PROCESS_CALL_CONFIG_INVALID`; W1/W4 are the positive controls that apply cleanly.
Inventory 24 → 26 → **24**.

---

## Extra — the NEW `abort_on_error=false` claim, graded LIVE

The correction introduced a claim about the **runtime**, not just about #175 terminality: *"the
parent run finishes normally instead of failing."* r1 only ever measured the `abort_on_error=true`
branch. Served contract text is machine-facing API, so this was graded by running it.

**Setup.** One child that always fails (`start -> exception(canary)`, authored as raw platform XML,
so it does not derive from the code under test), and three wrappers built through the public
`build_integration` boundary. Same child, same `wait=true`; only `abort` differs.

| arm | config | parent execution status | role |
|---|---|---|---|
| `W_true` | `abort_on_error=true`, failing child | **ERROR**, 1 error doc | positive control |
| `W_noop` | `abort_on_error=false`, GREEN child | **COMPLETE**, 0 error docs | negative control |
| `W_false` | `abort_on_error=false`, failing child | **COMPLETE**, 0 error docs | **the served claim** |

Execution ids: `execution-25600896-…` (W_false), `execution-86182a8d-…` (W_true),
`execution-81a861ca-…` (W_noop), all `-2026.08.20`.

**Both controls valid**, so the `W_false` reading is meaningful: the probe can see a failing parent
(W_true) and a passing parent (W_noop).

**The child really failed under both wrappers** — `troubleshoot_execution(action="error_details")`
returned three canary-bearing error executions:

```
_TEST_QA175R6 failing child 887e50   Process terminating -- some documents reached an exception
                                     which is set to halt all documents: QA175R6_CHILD_FAILED_887e50
_TEST_QA175R6 failing child 887e50   (same — the second invocation)
_TEST_QA175R6 Wtrue 887e50           Child Synchronous Process Call: _TEST_QA175R6 failing child
                                     887e50 [execution-86182a8d-…(shape2)] call ended in error: com.
```

Two child ERROR records (one per wrapper), but **only `Wtrue` appears as a parent-level error**.
`Wfalse` is absent from the error list entirely.

**Verdict: the served claim is platform-true.** With `abort_on_error=false` the parent run finishes
normally despite the child failing; with `true` it fails. This also confirms the second half of the
note — the flag decides the parent's *outcome*, and the emitted graph (§4) shows there is provably
no successor shape it could be selecting instead.

---

## Extra — entry-point symmetry spot-check

`harness/175-placement-authority-siblings.py`: **0 / 10 divergences**, matching r5. All ten arms
(T1–T5 try/catch bodies, R1–R5 root sequences) agree between the authoring entry point and the
compiler on **both code and pointer**. **Zero accept/refuse asymmetries** — no document accepted by
one path and refused by the other.

**Consolidated note for #178:** this round surfaced **no new diagnostic-identity divergences**.
Nothing to add to the deferred corpus.

---

## Non-blocking notes (not findings; no work opened)

1. **Unserved sibling prose.** `process_flow_builder.py:3247` (comment) and `:4789`
   (`WrapperSubprocessBuilder` docstring) still carry the withdrawn wording "the parent continues
   past a child failure". Proven not served (see §3). Outside the blocking classes per CLAUDE.md.
   Worth folding into any future non-blocking batch touching that file; it does not merit one on its
   own. Side effect worth recording: this measurement **refines an r4 assumption** that builder
   docstrings are machine-facing contract — they are not.
2. **`tests/test_wave_gate.py`** gained a 3-line comment in this delta. It is comment-only and
   arithmetically correct (`words[at-3 : at+3]` with `at` = the verb's word index does yield three
   words before, the verb, and two after). No issue.

---

## Account and worktree hygiene

**Account** (`renera` → `traininghlibbochkarov-JKIY2X`, the disposable demo): 6 process components
created, 6 deleted (4 by the live abort probe, 2 by the inventory bar). Teardown filtered to
`status == "created"` and asserted on the `_TEST_QA175R6` / `_TEST_QA175R5` name prefixes before
deleting. Process inventory back to **24**. `_QA_FIXTURE_noop` (`31bb57cd-…`) referenced read-only
and untouched. Both runtimes ONLINE throughout.

**Worktree at exit (verbatim):**

```
$ git rev-parse HEAD
adb905a7cda227251936e67ef8599f500cb60ce9

$ git status --porcelain
(no output — EMPTY)

$ git grep -n "MUT-" -- 'src/**'
(no output, rc=1)
```

No tracked file was modified by this run. **I created no worktree and no stash.** My writes went
only to the session scratchpad, `agents/reports/` and `.claude/agent-memory/boomi-qa-tester/` —
both of the latter are gitignored (`.gitignore:131 agents/`, `.gitignore:47 .claude/`), which is why
the status stays empty.

*Note:* `git worktree list` shows two pre-existing detached worktrees
(`…/2d42c6a7-…/scratchpad/wt-b6fc6e42` @ `b6fc6e4` and `…/e9ff7746-…/scratchpad/wt-manifest` @
`a3d21e9`). **Neither was created by this run**, so I left them in place rather than risk destroying
another session's state. Flagging for whoever owns them.

---

## Instrument harvested

`.claude/agent-memory/boomi-qa-tester/harness/175-served-claim-sweep-pathaware.py` — runnable
standing bar, supersedes the key-blind `175-withdrawn-form-sweep.py` (README updated to mark the old
one SUPERSEDED). Arm A grades the corrected field notes; Arm B sweeps the whole served corpus. Every
rule is proven non-vacuous by seeding its canary into a real served payload at a real leaf, and the
tool-description corpus carries its own emptiness assert so it can never silently return a vacuous
zero the way the first attempt did.

Verified runnable at HEAD: `RESULT A1_withdrawn=0 (want 0) A2=True C1=True C2=True vacuous=0
corpus_hits=25`.
