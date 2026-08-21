# Stage-1 QA round 3 — issue #178 (fix-delta re-run for the Stage-2 round-2 correction)

Loop 1 (Stage-1 QA), evaluation 3. **Verdict: regression-clean, every round-2 finding verified
resolved, TWO Low residues.**

- Tree: `1618f99e48ee5adbf4488970b660a0f273bd4c1b` plus the uncommitted delta. Freeze stamps
  `3ef7c5a828ef` (live arms) / `5702803c4b60` (direct-API arms), `files=133` / `29`,
  PRE -> POST -> EXIT stable.
- Suite not re-run this round by instruction; the delta measured **10215 passed, 17 skipped**.
- Account returned to its exact starting inventory (22 / 24 / 22; 26 of 26 created components
  deleted, twice).

## Regression sweep — clean, third consecutive round

All five emitted-XML digests match round 1 and round 2 exactly: `82f4f599f4665d96`,
`cd13b744e2693f2a`, `f4d122dccb2ae6bd`, `e7f52c06501b6ed6`, `f26a1112f5f7a078`. Both frozen
2026-07-11 M8 oracles reproduce byte for byte. Full leaf-diff of the 36-arm
archetype/recipe/compose matrix versus round 2: **one differing leaf, the probe's own freeze
stamp**. The `mode="python"` payload shape is inert on real payloads.

Supporting check, with its own limit stated: the model module defines NO Enum at all (`grep -c` =
0), so the python/json dump difference is confined to container types. QA flagged that its
model-walker reached only 2 models, so it cites the file-level grep rather than the walk.

No mutation on refusal: 22 -> 22 with `mutation_performed: false`, positive control +1 per genuine
create.

## The one-shot iterable regression — closed, with a non-vacuity control

| case | `1618f99` | fix tree |
| --- | --- | --- |
| generator over `body.steps` | REFUSED `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | ACCEPTED, cfg_nodes=4 |
| plain iterator over `body.steps` | REFUSED, same | ACCEPTED, cfg_nodes=4 |
| generator inside a Branch leg | REFUSED, same | ACCEPTED, cfg_nodes=6 |
| **EMPTY generator (control)** | REFUSED | **REFUSED** |

The empty-generator control is what makes the result meaningful: the cardinality gate still fires,
so accepting the populated generators is not a gate that quietly stopped working.

## Round-1 and round-2 properties all still hold

`datetime` in a `str` field REFUSED `PROCESS_IR_SCHEMA_INVALID` at `/body/steps/1/text`; `bytes`
ACCEPTED (correct — the parser accepts it, so the compile entry must too, and QA deliberately did
not report it as a defect); benign `str`-subclass and node-subclass shapes still compile; zero
canaries in any refusal, warning or log, with the scanner proven live.

## Round-2 findings verified resolved

- **QA-178-r2-01** — all three real entry points now agree 9/9: `compile_process_ir_v1`,
  `parse_and_compile(python dump)`, and the new `compile_process_ir_model_v1`. QA had drafted a
  further finding that the workflow call site still diverged and **withdrew it** after re-reading
  the call site: its probe column was simulating the old hand-dump, and that call site no longer
  exists.
- **QA-178-r2-02** — the pre-fix leak reproduced verbatim on a hybrid tree
  (`input_value=b'QA178CANARYSECRET-sk_live_0ff1ce'` in the warning text), absent at `1618f99` and
  on the fix tree, measured in fresh isolated processes with the canonical-JSON canary hit as the
  positive control. QA also validated the change's "changes no emitted byte" claim rather than
  accepting it: **0 of 6 canonical digests moved**.
- **QA-178-r2-03** — the inaccurate docstring claim is gone.

QA disclosed that its round-2 in-process probe and this round's isolated probe initially disagreed
about `818e0da`, and resolved it by building a hybrid tree to isolate the variable rather than
picking a side; the difference was real and attributable to `models/process_ir.py`.

## Findings

### QA-178-r3-01 — the compile entry is still a destructive read (Low, not reachable today)

Collapsing two dumps into one makes the FIRST compile correct, but one dump still consumes a
one-shot iterable. Measured: compiling the same model object twice gives ACCEPTED (cfg_nodes=4)
then REFUSED, and the caller's `body.steps` drains to 0. The "same model, multiple compiles" shape
is real — a live typed apply logged one model object entering the public entry three times, from
`process_materialization.py:627` and `canonical_process_apply.py:247` (twice).

The two legs do not currently meet: production models come from the intake parse with materialised
containers.

**Disposition: fixed as a documented CONTRACT, not a code change.** It cannot be sequenced away —
the identical drain was measured on the pre-#178 dump shape, so it is a property of re-parsing at
all rather than of any implementation, and Python offers no way to read a one-shot iterable twice.
The public entry now states the read-exactly-once precondition; two tests pin it, including that
the model returned by `compile_process_ir_model_v1` is the escape hatch and compiles repeatedly.

### QA-178-r3-02 — the baseline attestation was inexact (Low, audit-record integrity)

The round-3 dispatch called `1618f99` "the tree you tested in round 2". It also carries three
corrections made in response to round 2 — `authoring/workflow.py`, `recipes/composer.py`, and the
`canonical_process_ir_json` change — none of which were in the tree round 2 tested. So three
applied corrections reached a committed baseline without a QA round of their own.

No harm resulted; round 3 validated all three. But at face value the attestation would have
directed QA to skip exactly the code that changed.

**Disposition: fixed.** The claim was mine and it was wrong; `git diff 818e0da 1618f99 -- src/`
confirms the three files. The ledger's QA table is corrected, an *Attestation correction* section
records what happened, and diffing the named baseline against the previous one before attesting is
adopted for the rest of the slice.

## Residual observation, not a finding

`authoring/workflow.py:379` still parses a `mode="json"` dump at intake normalization — the same
mechanism the new docstring calls "WRONG here" — but its input comes from the request-intake parse
one step earlier, so it cannot carry a mutated value. QA measured that reasoning rather than
assuming it.

## Instruments persisted

`.claude/agent-memory/boomi-qa-tester/harness/oneshot-iterable-and-idempotency.py`,
`isolated-serializer-warning-probe.py`, `canonical-json-equivalence.py`; round record in
`issue178-rounds-index.md`.
