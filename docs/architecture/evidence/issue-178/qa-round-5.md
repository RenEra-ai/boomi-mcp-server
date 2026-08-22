# Stage-1 QA round 5 — issue #178 (validating the CRITICAL fix for `QA-178-r4-01`)

Loop 1 (Stage-1 QA), evaluation 5. **Verdict: CLEAN. `QA-178-r4-01` is closed. No new defects.**

- Attested SHA `77ed08a9dd887023c1a0a442b372bc874a2e4aea`, verified by diff rather than trusted:
  `fc3cb0e..77ed08a -- src/` is exactly `pipeline.py` (+10/-7), nothing outside `src/`, `tests/`,
  `docs/architecture/`.
- The tree read `clean` at PRE and `worktree_moved=False` at ATTEST on EVERY run — the freeze held.
  QA notes this was the first round it did not have to reason about run integrity, which is the
  direct result of the `QA-178-r4-02` corrective practice.
- Account back to 22 / 24 / 22, 26 of 26 created components deleted, repo untouched by QA.

## The exploit is closed, and the ordinary path was not traded for it

Both public model entries, QA's own round-4 harness:

| dump failure kind | `fc3cb0e` | `77ed08a` |
| --- | --- | --- |
| parser-typed (subclass `model_dump`) — the exploit | forged code at the caller's pointer, secret served | `PROCESS_IR_COMPILE_INTERNAL`, path `''` |
| `PydanticSerializationError` — the ordinary path | `PROCESS_IR_COMPILE_INTERNAL` | `PROCESS_IR_COMPILE_INTERNAL` |
| legal control | ACCEPTED | ACCEPTED |

Served verbatim at `77ed08a`, both entries: `message='compiler invariant violated'`,
`remediation='This is a compiler defect, not a problem with the authored payload — please report it
with the authored path.'`, `phase='schema'`, `path=''`. The canary is absent from message,
remediation, warnings, logs and stderr — **with the scanner control firing**, so the zeros are
observations rather than a blind probe.

QA also confirmed the SURVIVING `except ProcessIRValidationError` in `_parse_payload_for_compile` is
the right one to keep: it wraps the actual parse, where a parser-typed error genuinely is
parser-authored. The deleted arm sat around the DUMP, before parsing. That is precisely the
distinction, and both sides are now correct.

## What deleting a branch changed — 12 exception types, 0 problems

| raised by the dump | `fc3cb0e` | `77ed08a` |
| --- | --- | --- |
| `ProcessIRValidationError` (the deleted arm) | **LEAK** | -> `ProcessIRCompileError`, no leak |
| `ProcessIRCompileError` (forged) | normalised | normalised |
| `RuntimeError`, `ValueError`, `TypeError`, `AttributeError`, `RecursionError`, `MemoryError` | normalised | normalised |
| `KeyboardInterrupt`, `SystemExit`, `asyncio.CancelledError`, `GeneratorExit` | escape untouched | escape untouched |

**8/8 `Exception`-derived types convert to `ProcessIRCompileError`** — the one family every
production handler catches — with neither the canary nor a forged code surviving. **4/4
`BaseException`-only types correctly escape**; swallowing those would have been a worse defect than
the one being fixed, so QA scored them as expected escapes rather than failures. A forged
`ProcessIRCompileError` raised by the dump is also normalised rather than passed through.

## End-to-end coverage and the standing properties — clean

Five emitted-XML digests exact for the FIFTH consecutive round: `82f4f599f4665d96`,
`cd13b744e2693f2a`, `f4d122dccb2ae6bd`, `e7f52c06501b6ed6`, `f26a1112f5f7a078`. Both frozen M8
oracles byte-for-byte. 36-arm matrix leaf-diff versus round 4: one leaf, the probe's own stamp.
Refusals at `action="apply"` held inventory 22 -> 22 with `mutation_performed: false`, positive
control five real creates. Properties: `datetime` REFUSED at `/body/steps/1/text`; `bytes` ACCEPTED;
one-shot generator ACCEPTED with the full step count (cfg_nodes 4, and 6 nested); empty-generator
control still REFUSED; **9/9 parity** across the three entry points. The generator non-idempotency is
unchanged and is the known `QA-178-r3-01` documented contract, not a new finding.

## Discriminating? Split, and stated

The end-to-end sweep is meaningful — it protects the shared path the change sits on. The
classification change itself remains **unreachable from the MCP boundary**: both dump-failure
constructions require a caller-supplied Python subclass, and JSON delivers neither.
`PROCESS_IR_COMPILE_INTERNAL` appears in 0 of the ~45 live served envelopes. The direct-API work is
where the evidence lives, and a boundary-only green would have meant nothing.

## QA-178-r5-01 — QA's correction to its OWN round-4 evidence (Low, non-blocking)

In round 4 QA argued the E-matrix survived the mid-round edit because its `files=133
code=11069ffe3050` matched a run taken while the tree was clean. **That reasoning was unsound.**
Measured this round: `pipeline.py` is imported lazily inside function bodies, so it is NOT in the
133-file pinned set — it appears in the "50 file(s) lazily loaded" line — and the `code=` hash is
byte-identical across `fc3cb0e` and `77ed08a` even though `pipeline.py` differs between them. The
hash proved the 133 eagerly-loaded files matched; it said nothing about the file under test.

The round-4 conclusion still stands, on different grounds: that E-matrix was byte-identical to
round 3's, and none of its arms reach the changed code. The durable fix is in the harness — attest a
live run with `tree=clean` plus `worktree_moved=False`, and import the module under test explicitly
before pinning if it must be pinned.

Recorded because the implementer acted on the round-4 report, and one line of its reasoning should
not stay load-bearing when it is not sound.

## On the tiering

QA notes it does not dispute Critical over its own High or the reviewer's P2: it offered the
decision and considers the stricter anchor defensible. Its reasoning for High was that the planted
value is supplied and received by the same in-process caller, so no privilege boundary is crossed;
the durable defect was caller-controlled text entering a served, logged diagnostic channel. That is
the part the fix closes, and it is closed.

## Instruments persisted

`.claude/agent-memory/boomi-qa-tester/harness/exception-escape-battery.py`; round record in
`issue178-rounds-index.md`.
