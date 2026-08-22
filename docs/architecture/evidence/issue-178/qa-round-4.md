# Stage-1 QA round 4 — issue #178 (validating the §6 architect-review round-1 fix delta)

Loop 1 (Stage-1 QA), evaluation 4. **Verdict: regression-clean, but `fc3cb0e` carries a
forged-diagnostic leak — ONE CRITICAL-tier finding plus one Low process finding.**

- Attested SHA `fc3cb0e5cfe1c7ea3191df7c3cb93a2696ef0fec`.
- **Baseline verified rather than trusted**, per `QA-178-r3-02`'s adopted practice:
  `git diff 3cb564d..fc3cb0e -- src/` is exactly the two named files, and nothing in the range falls
  outside `src/`, `docs/architecture/`, `tests/`.
- Account returned to its starting inventory (22 / 24 / 22; 26 of 26 deleted). The repo was not
  modified by QA.

## Regression sweep — clean, fourth consecutive round

All five emitted-XML digests exact: `82f4f599f4665d96`, `cd13b744e2693f2a`, `f4d122dccb2ae6bd`,
`e7f52c06501b6ed6`, `f26a1112f5f7a078`. Both frozen 2026-07-11 M8 oracles byte-for-byte. 36-arm
matrix leaf-diff versus round 3: one differing leaf, the probe's own freeze stamp. Refusals at
`action="apply"` held inventory at 22 -> 22 with `mutation_performed: false`, positive control five
real creates.

Previously-validated properties all hold at `fc3cb0e`: `datetime` REFUSED at `/body/steps/1/text`;
`bytes` ACCEPTED; a generator over `body.steps` ACCEPTED with the full step count (cfg_nodes 4, and 6
nested in a Branch leg); the empty-generator control still REFUSED; 9/9 parity across all three
entry points.

## QA-178-r4-01 — forged compiler diagnostic (CRITICAL tier)

Moving the guard out of `raw_process_ir_payload` into the compile entry introduced a
`ProcessIRValidationError` arm that forwards the exception VERBATIM. Parsing has not started at that
point, so an error of that type cannot really be parser-authored — the arm trusted the TYPE. A
`ProcessIRV1` subclass whose `model_dump` raises a parser-typed error therefore injects its own
code, pointer, message and remediation into the compiler's authoritative error channel:

```
codes       : ['QA178_FORGED_CODE']
paths       : ['/QA178/forged/pointer']
phases      : ['schema']
messages    : ['forged parser message carrying QA178INTERNALCANARY-sk_live_b0rk3d']
remediations: ['forged remediation carrying QA178INTERNALCANARY-sk_live_b0rk3d']
```

Three-tree A/B, both public model entries, identical results on each:

| dump failure kind | `3cb564d` | `fc3cb0e` | working tree |
| --- | --- | --- | --- |
| `PydanticSerializationError` | `PROCESS_IR_SCHEMA_INVALID` | `PROCESS_IR_COMPILE_INTERNAL` | `PROCESS_IR_COMPILE_INTERNAL` |
| parser-typed (subclass `model_dump`) | `PROCESS_IR_SCHEMA_INVALID`, no leak | **forged code + secret served** | `PROCESS_IR_COMPILE_INTERNAL`, no leak |

So it is a regression INTRODUCED by the §6 round-1 delta: at `3cb564d` the helper normalised every
exception, which incidentally closed it.

**Tier.** QA labeled it **High** and explicitly offered the retier (*"the raw label is yours to
set"*), noting that the planted value is supplied and received by the same in-process caller so no
privilege boundary is crossed. The STRICTER reading is taken: the tier rules derive **Critical** from
a source label of High, and arbitrary caller-controlled text entering a served, logged diagnostic
channel is exactly what the AR2-01 value-free contract exists to prevent. Critical is not deferrable
and not closable over, so the fix takes a QA round of its own plus a delta review before closure.

The same defect was independently reported by the Stage-2 support review as `L2R8-01` at P2. One
distinct defect, two raw findings, each with its own disposition; the Critical tier governs.

## QA-178-r4-02 — the worktree was edited mid-round (Low, run integrity)

`src/boomi_mcp/compiler/process_ir/pipeline.py` — the file under test — gained uncommitted edits
while the round was reading it. The `code=` freeze hashes let QA prove which results survived: the
apply matrix ran while the tree was provably clean (`fc3cb0e5cfe1/clean/11069ffe3050`) and the E
matrix reported an identical file set and identical code hash, so both live matrices stand as
evidence for `fc3cb0e`. Everything after that was re-run against a pinned `git archive` extraction.

QA's assessment that this was luck is accepted. Had the edit landed seconds earlier it would have
voided the live matrices and cost roughly twelve minutes of live account work — the same class as
the #153 r4/r5 losses. The fault is the implementer's: the branch-removal fix was applied while the
round was in flight, which is the hazard this repo had already recorded as *never write the worktree
while a gate reads it*. Knowing the rule did not prevent it, so the correction is procedural: while
any gate holds the tree, corrections are staged in the scratchpad and applied only after the gate
reports.

## Is this round discriminating? Partly — the honest split

**The classification change is NOT observable at the MCP boundary.** Forcing a `model_dump` to raise
is hard: under `warnings=False`, pydantic 2.12.3 returns unknown objects untouched — a raising
`__len__`, `__iter__`, `__getattr__`, `__repr__`, a cycle, a self-cycle and a 5000-deep nest all dump
normally. The only two constructions that raise require the caller to supply a Python SUBCLASS (a
node with a raising `@model_serializer`, or a model overriding `model_dump`), and JSON can deliver
neither. Confirmed empirically: `PROCESS_IR_COMPILE_INTERNAL` appears in 0 of the ~45 live served
envelopes. The regression sweep is meaningful because it protects the shared path, but a green on the
classification item at the boundary would have meant nothing — which is why QA went to the direct API.

## QA's disclosure of a defect in its own probe

QA's first attempt reported that an object with a raising `__len__` made the dump raise. It did not —
the *reporting line* called `len()` on the planted object. That false positive is what prompted a
harder search, and the harder search is what found `QA-178-r4-01`.

## Instruments persisted

`.claude/agent-memory/boomi-qa-tester/harness/internal-vs-schema-classification.py`,
`find-a-raising-model-dump.py`; round record in `issue178-rounds-index.md`.
