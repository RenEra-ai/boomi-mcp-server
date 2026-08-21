# Stage-1 QA round 2 — issue #178 (fix-delta re-run for the Stage-2 round-1 correction)

Loop 1 (Stage-1 QA), evaluation 2. Live, through the public MCP tool boundary, against the
`renera` account. **Verdict: regression-clean, hole closed, THREE findings — none blocking at the
tool boundary.**

- Tree: `818e0dae78ea729768add36517e5ef01f657f068` plus the uncommitted fix delta. Freeze stamps
  `818e0dae78ea/dirty:4db27b9d66ac/code=c4bc8a19804f` (live arms) and `026385aa589b` (direct-API
  arms); PRE -> POST -> EXIT stable throughout.
- Suite NOT re-run this round by instruction; the measurement it relies on is round 2's
  **10212 passed, 17 skipped, 0 failed** on the same delta.
- Account returned to its exact starting inventory (22 processes / 24 connections / 22 operations;
  26 of 26 created components deleted).
- **Baseline attestation verified rather than assumed:** `git archive 818e0da src` diffed against
  the round-1 tree copy is IDENTICAL (excluding the one file carrying round-1 census injections),
  so the round-1 captures really are `818e0da` evidence and were safe to reuse.

## Regression sweep at the tool boundary — clean

`warnings="error"` refused nothing legal. A full leaf-diff of the 36-arm archetype/recipe/compose
matrix across the fix delta shows **one differing leaf, and it is the probe's own freeze stamp**.
Both frozen 2026-07-11 M8 oracles still reproduce byte for byte (`composed_db_to_api_fanout` 10
components, `cache_handoff_staged_fanout` 11).

The 9-arm mutating apply matrix is arm-for-arm identical to round 1, and all five emitted process
XML digests match exactly:

| process | round 1 (`818e0da`) | round 2 (fix tree) |
| --- | --- | --- |
| typed ProcessIR `_D` | `82f4f599f4665d96` | `82f4f599f4665d96` |
| legacy `wrapper_parent` (policy-bearing) | `cd13b744e2693f2a` | `cd13b744e2693f2a` |
| `DB to API Sync` | `f4d122dccb2ae6bd` | `f4d122dccb2ae6bd` |
| `L DB to API Sync` (Try/Catch) | `e7f52c06501b6ed6` | `e7f52c06501b6ed6` |
| `A API to API Sync` (sync_pipeline, policy-bearing) | `f26a1112f5f7a078` | `f26a1112f5f7a078` |

No mutation on refusal: inventory 22 -> 22 with `mutation_performed: false`, and a positive control
in the same run moved inventory +1 per genuine create.

## The hole is closed, with the coercion control that makes it mean something

Direct API, `/body/steps/1/text` on a legal `[source, message, target, stop]` root:

| case | `818e0da` | fix tree | coercion control (json dump renders -> parser accepts?) |
| --- | --- | --- | --- |
| `text = datetime(2020,1,1)` | ACCEPTED | REFUSED `PROCESS_IR_SCHEMA_INVALID` @ `/body/steps/1/text`, `phase="schema"` | `"2020-01-01T00:00:00"` -> **accepts** |
| `text = b"…canary…"` | ACCEPTED | REFUSED (at measurement time) | `"QA178BYTESCANARY-…"` -> **accepts** |
| `str` subclass (benign) | COMPILED | COMPILED | — |
| node subclass in the union (benign) | COMPILED | COMPILED | — |
| `int` / `None` / `bool` / `int` in `Optional[str]` | REFUSED | REFUSED (unchanged) | dump does not coerce |

A legal document emits ZERO serializer warnings, which is why `warnings="error"` is free on the
happy path. Positive control `T0` still compiles clean against a real symbol table (4 CFG nodes) —
"still compiles", not "fails differently".

**Note on the `bytes` row.** It was refused at measurement time and is NOT refused after this
round's correction; see QA-178-r2-01 below. That is deliberate: the parser accepts `bytes`, so the
compile entry must too.

## No-leak — clean, with a proven-live scanner

Across all nine cases: 0 canary hits in refusal diagnostics, captured `warnings`, and captured log
records; 0 warnings emitted during compile (the serializer raises rather than warns, and the raise
is caught). The scanner is not blind — its non-vacuity control fires on `T1`–`T4`
(`bytes_canary`, `bytes_secret`, `str_canary`, `str_secret`, `datetime_literal`, `int_literal`).

## Findings

### QA-178-r2-01 — entry-point parity only partially restored (Medium / Standard, runtime behavior)

Measured, same underlying document state:

| state | `compile_process_ir_v1` | `parse_and_compile(python dump)` | `parse_and_compile(json dump)` — the shape `workflow.py` used |
| --- | --- | --- | --- |
| legal | ACCEPTED | ACCEPTED | ACCEPTED |
| `text = bytes` | REFUSED | ACCEPTED | ACCEPTED |
| `text = datetime` | REFUSED | REFUSED | ACCEPTED |

At `818e0da` the compile column was ACCEPTED/ACCEPTED/ACCEPTED — the two entries agreed, wrongly.
After the round-1 fix they DISAGREE on `bytes`, because `parse_process_ir_v1` is lax and coerces
`bytes` to `str` while only the compile entry got `strict=True`. Separately, three `model_dump`
calls fed a parse/compile and only one was hardened.

**Disposition: fixed.** The compile entry now mirrors the parser against the raw state instead of
strict-validating — a rule stricter than its own authority is the DC-175-E mechanism reintroduced
one layer up — and the model-to-payload conversion moved into one place,
`compile_process_ir_model_v1`, so no call site picks its own dump mode.

### QA-178-r2-02 — `canonical_process_ir_json` leaks the authored value into a serializer warning (Low, pre-existing)

It dumped with pydantic's default `warnings=True`; with a `bytes` canary in `message.text` the
warning text contained `QA178CANARYSECRET` and `sk_live_0ff1ce`. Measured IDENTICAL at `818e0da`,
so pre-existing rather than introduced. Not a served output.

**Disposition: fixed**, plus a sibling sweep — `recipes/composer.py` (x2) and
`authoring/workflow.py:1750` carried the same unhardened dump and were hardened with it. Zero
unhardened ProcessIR dump sites remain in `src/`.

### QA-178-r2-03 — a test docstring claim contradicted by measurement (Low, not served)

The new test's prose said the same raw value through `parse_and_compile_process_ir_v1` is refused;
measured `datetime` refused, `bytes` ACCEPTED. The assertions were correct; only the prose
overstated. **Disposition: fixed** — rewritten, and the `bytes` case became its own test asserting
the compile entry must NOT be stricter than the parser.

## What was and was not discriminating

Stated by QA rather than inferred: the tool-boundary regression sweep IS fully discriminating for
the named risk — round 1's census proved these arms reach `compile_process_ir_v1`, and round 1's
seeded control proved they visibly break when it refuses, so 45 identical arms is real evidence
that `warnings="error"` refuses nothing legal. The direct-API battery is discriminating because
every case carries its coercion control and the A/B shows the closed cases flipping. NOT
discriminating and deliberately not re-run: the round-1 A/C rows, which the intake parse settles
before any compile entry.

## Instrument defect disclosed by QA rather than buried

The first run of the type-faithfulness battery called `F.loaded()` before importing the code under
test, so it pinned zero files and its attestation was vacuous. The ordering was fixed, an assertion
added, and both arms re-run; every number above comes from the corrected runs (`files=22`).

Instruments persisted: `.claude/agent-memory/boomi-qa-tester/harness/model-type-faithfulness-battery.py`,
`model-dump-site-audit.py`. Round record updated in
`.claude/agent-memory/boomi-qa-tester/issue178-rounds-index.md`.
