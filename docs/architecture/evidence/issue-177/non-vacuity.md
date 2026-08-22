# Non-vacuity witnesses — issue #177

Every invariant this slice ships must be shown to FAIL when the real historical defect is
reintroduced. The witness uses the real text/shape, never a paraphrase (#175 shipped a guard that
passed its own witness *and* the restored defect because it excluded newlines).

Each mutant is hand-run with **no gate reading the worktree**. Recorded per mutant: the exact file
and edit, the guard node that went red with its first assertion line quoted, and the restored-green
confirmation.

*(rows appended as each invariant lands)*

## Invariant 3 — §8 capability table key-AND-state equality

Guard: `tests/test_process_ir_authoring_contract_parity.py::test_the_published_capability_table_matches_the_registry_exactly`,
parsing through `_parse_capability_states()`.

### Mutant 3a — a real state drifts (the DC-175-D shape)

Edit: `docs/architecture/PROCESS_IR_V1.md`, the real `process_call_return_path_binding` row (line 380),
State cell `gated` → `**supported**`. The runtime manifest is left at `gated`.

RED, first assertion line quoted verbatim from the run:

```
>       assert published == dict(PROCESS_IR_V1_CAPABILITIES), {
E       AssertionError: {'in the doc only': [], 'in the registry only': [], 'state disagrees': [('process_call_return_path_binding', 'supported', 'gated')]}
```

The two empty lists are the load-bearing part: the KEY SET is untouched, so the predecessor
key-only guard cannot see this defect. Measured directly against the mutant — *measured here*:

```
OLD guard set-equality holds on the mutant: True
OLD guard floor len(published)>=25: True
```

So the old guard passed the mutant on both of its assertions, and the new one fails it. Restored
byte-for-byte (`git diff --quiet` clean) and re-run green.

### Mutant 3b — the authority is renamed (fail-closed direction)

Edit: same file, `| Capability | State | Owner |` → `| Capability | Status | Owner |`.

RED, quoted verbatim:

```
E           AssertionError: expected exactly one '| Capability | State | Owner |' row, found 0
```

It fails BEFORE row extraction, which is the point: a lenient parser would have matched zero rows
and compared an empty mapping, passing trivially. Restored byte-for-byte and re-run green.

### Green after restore

```
62 passed in 0.66s
```

(`tests/test_process_ir_authoring_contract_parity.py`, 60 nodes at the step-0 baseline plus the two
in-memory controls this invariant adds.)

## Invariant 1 — every emittable code carries complete served text

Guard: `tests/test_process_ir_served_text_enforcement.py::test_every_emittable_process_ir_code_has_complete_served_text`,
reading the demand side through `tests/_process_ir_diagnostic_emissions.py`.

### Mutant 1 — the real `L3-04` registration loss

Edit: `src/boomi_mcp/compiler/process_ir/diagnostics.py`, delete the
`PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED` entry from **both**
`_MESSAGES` and `_REMEDIATION` (2 entries removed). The real emissions in
`body_capabilities.py` are left untouched, exactly as in the historical defect.

RED, quoted verbatim:

```
E       AssertionError: [('compiler', 'PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED')]
E       assert [('compiler',...UNSUPPORTED')] == []
```

Why removing BOTH entries is what makes this faithful — *measured here* under the mutant:

```
accessor still green (symmetric+non-blank): compiler rows = 32
code present in the served UNION under the mutant: True
code present in the COMPILER table under the mutant: False
```

So the fail-closed accessor added by this slice stays green (the two tables remain
symmetric), and the parser still registers the code, meaning **a union-based guard passes
the mutant**. Only the producer-aware direction fails. This was not theoretical: the first
version of this guard compared against the merged union and DID pass the mutant — the
witness caught a defect in the guard, and the guard was rewritten to compare each layer
against its own table.

Collateral RED under the mutant: `test_the_guard_fails_when_a_registration_is_removed`,
which pre-asserts the code is registered before stripping it in memory. Expected.

Restored byte-for-byte (the code is registered twice again in `diagnostics.py`) and re-run
green: `8 passed in 1.24s`.

## Invariant 2 — manifest-keyed capability enforcement

Guard: `tests/test_process_ir_capability_enforcement.py::test_every_gated_capability_is_refused`,
via the `process_call_return_path_binding` witness in
`tests/_process_ir_capability_witnesses.py`.

### Mutant 2 — the real `L2-r6-01` crash

Edit: `src/boomi_mcp/models/process_ir.py`, replace the offending-index selection with the
historical spelling:

```python
offending = next(
    i for i, kind in enumerate(kinds) if kind != "process_call"
)
```

RED, quoted verbatim:

```
E       AssertionError: [('process_call_return_path_binding', 'StopIteration: ')]
```

Both historical halves reproduced under the mutant — *measured here*:

| Input (verbatim) | Restored defect | Correct behaviour |
| --- | --- | --- |
| `[process_call a, process_call b]` | `StopIteration` escapes the validator untyped | `PROCESS_IR_CAPABILITY_PROCESS_CALL_RETURN_PATH_BINDING_UNSUPPORTED` at `/body/steps/1` |
| `[process_call a, process_call b, stop]` | points at `/body/steps/2` (the trailing stop) | points at `/body/steps/1` (the second call) |

The witness was STRENGTHENED after this run: its first version carried only the all-call
input, so it caught the crash but not the pointer half. It now carries both documents and
was re-run against the re-applied mutant to confirm it still goes red, then restored.

Restored byte-for-byte and re-run green: `7 passed in 0.13s`.
