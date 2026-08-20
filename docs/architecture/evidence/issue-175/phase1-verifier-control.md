# Phase-1 evidence — the continuation invariant is non-vacuous, and two-directional

All output below is quoted from the run, not re-keyed. Measured on the branch tip that
carries ONLY the graph-verifier change (the structural fix), before any grammar, lowering,
emitter or builder edit — so nothing here depends on the rest of the slice.

## 1. Negative control — the frozen pre-baseline bytes

Run against `prefix-goldens/` (captured at `3fd5027e16d4f9fe5377884a0140909a3b4d1e67`, before
the first source edit) and the permanent in-tree fixture. Every artifact this repository
generated at the baseline fails the new rule, and **fails nothing else** — the reported code
set is exactly one element in all four cases:

```
processcall_standalone_parent.xml:       codes=['PROCESS_CALL_ORPHAN_CONTINUATION'] shapes=['shape2']
process_ir_rich_branch_process_call.xml: codes=['PROCESS_CALL_ORPHAN_CONTINUATION'] shapes=['shape3', 'shape5']
wrapper_flow.process.xml:                codes=['PROCESS_CALL_ORPHAN_CONTINUATION'] shapes=['shape2', 'shape3']
processcall_orphan_continuation.xml:     codes=['PROCESS_CALL_ORPHAN_CONTINUATION'] shapes=['shape2']
```

This is what makes the guard non-vacuous: it rejects artifacts this repository actually
produced and shipped. `wrapper_flow.process.xml` reports TWO offending shapes, which also
records that the multi-call wrapper chain was reachable.

## 2. Two-directional mutation control — the live UI-built capture

The positive oracle is `tests/fixtures/live_xml/m11/process_doccacheretrieve_loadalldoc_variant.xml`,
UI-built and frozen long before this baseline. Its `shape10` is the only Process Call in the
tree that declares a return path, and the only one with an outgoing connection — and its
dragpoint carries `identifier="shape233"`, equal to the return path's `childShapeName`.

The mutation touches ONLY `shape10`'s own element and splices it back; the restore is asserted
byte-identical to the original file, so the control cannot pass by accident:

```
BASE            : NONE (clean)
MUTANT (shape10): ['shape10']
RESTORED        : NONE (clean)
```

Both directions matter. BASE proves the guard does not fire on valid platform-produced XML
(the failure mode a one-sided guard hides); MUTANT proves it fires when the declared return
path is removed while the connection stays; RESTORED proves the fire was caused by that
change and nothing else.

`process_dpp_profile_decision_flow.xml` (a fifth terminal-form Process Call, `shape61`) is
also clean at BASE.

**Harness note, recorded because it nearly produced a false result.** A first attempt mutated
the document with a whole-file `replace('<returnpaths/>', …, 1)`, which rewrote `shape4`'s
empty element — an earlier terminal call — instead of `shape10`'s. The RESTORED leg then still
reported `shape10` and looked like a verifier defect. The harness was wrong, not the verifier.
The measurement above is the corrected one.

## 3. Reachability inventory — what the invariant alone turns red

With only the verifier changed, the existing suite reports **22 failures** across
`test_wrapper_subprocess_builder.py`, `test_process_ir_rich_control_bodies.py`,
`test_process_emitter_parity.py`, `test_process_ir_compiler.py` and
`test_legacy_adapter_cutover.py`. Because `emit_process` runs `verify_process_graph` and
raises on errors, that red list IS the inventory of surfaces emitting the defect — it was not
enumerated by hand.

## 4. The legacy catch leg — the SECOND instance of the defect class

Measured through `ProcessFlowBuilder.build` on the corpus DLQ config. The trailing terminal is
an Exception throw when `catch_exception` is present, else a catch-row Stop when
`catch_notify` is, else nothing — and the DLQ route points at that terminal whenever it
exists:

```
bare error_subprocess_ref                          clean   shapes=[start, catcherrors, connectoraction, connectoraction, stop, processcall]
catch_notify + error_subprocess_ref                ['shape7']   shapes=[..., notify, processcall, stop]
error_subprocess_ref + catch_exception             ['shape6']   shapes=[..., processcall, exception]
notify + error_subprocess_ref + catch_exception    ['shape7']   shapes=[..., notify, processcall, exception]
document_cache_ref + catch_exception (control)     clean   shapes=[..., doccacheload, exception]
```

Three live-reachable compositions carry the defect; the bare DLQ-only leg does not (it is
already the terminal form). This is a DIFFERENT subsystem from the canonical emission chain
but the SAME (mechanism, runtime-authority) pair, which is what makes it the second instance
of DC-175-A and the structural fix mandatory in this batch rather than an instance patch.

The `document_cache_ref` row is the over-firing control: a Document Cache load legitimately
continues downstream, it is deliberately excluded from the always-terminal set, and it stays
clean with the same trailing Exception that makes the Process Call row fail. The guard
discriminates on the declared return path, not on "something followed a shape".

**None of these three compositions was covered by an existing test that runs the verifier** —
which is why the 22-failure inventory in §3 does not include them. Closing that gap is part of
this slice.
