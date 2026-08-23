# Issue #154 golden-case provenance

One row per case document in this directory. The completion workflow's clean-room
fixture rule requires each fixture's served field names and shapes to derive from a source
**causally independent of the implementation under test** — and requires the provenance to be
recorded rather than assumed. Where a shape has no independent oracle, that is stated here rather
than glossed.

## What is and is not claimed

These goldens are rendered by the ProcessIR compiler, so **the bytes are not their own oracle**.
They are regression pins. What makes each one trustworthy is the evidence in the `Oracle` column
plus three checks that hold for every row:

1. `verify_process_graph` accepts the emitted XML (0 errors, all six);
2. the emission-plan and CFG invariants run inside `compile_process_ir_v1`, so a golden that
   compiles at all has already passed them;
3. the bytes are stable across repeated renders in isolated child processes — the wave gate
   renders every active row twice and compares.

This is the same standing the #141 rich-control goldens have, and for the same stated reason: the
legacy builder cannot express most of these flows, so no legacy-parity oracle exists for them.

## Per-case oracle

| Case | Issue item | Oracle | Provenance class |
| --- | --- | --- | --- |
| `source_target_return_documents` | 3 | **Legacy-oracle differential.** The legacy builder emits this exact flow, and its committed golden `tests/fixtures/golden_xml/return_documents_terminal.xml` was produced BEFORE this slice's baseline `c41bcf08`. Measured: the shape sequence `[start, connectoraction, connectoraction, returndocuments]` is IDENTICAL between that legacy golden and this compiler output. | legacy-oracle parity capture |
| `try_flow_control` | 1 | **Legacy shape evidence.** `process_flow_builder.py:1185-1213` passes the COMPLETE linear `flow` list into `_emit_try_catch_shapes`; that list carries a `flowcontrol` entry whenever `flow_control` is enabled (`:970-980`). So the builder wraps this kind inside a process-scoped try body today. The wrapping LAYOUT is not differentiated here — only the placement is attested. | legacy-oracle parity capture (placement only) |
| `try_data_process` | 1 | **Legacy shape evidence**, same mechanism: the `flow` list carries a `dataprocess` entry for that transform mode (`:1002-1012`) and is wrapped whole. | legacy-oracle parity capture (placement only) |
| `try_return_documents` | 2 | **Legacy shape evidence.** The wrapped `flow` list's terminal entry is `_terminal_flow_entry(config)` (`:1168`), which emits `returndocuments` when `return_documents.enabled` is set — inside the same wrap. | legacy-oracle parity capture (placement only) |
| `catch_cache_put_exception` | 4 | **Legacy shape evidence.** The DLQ catch leg writes to a document cache and then ends the path; `catch_exception` (issue #108, M10.4) makes that ending an explicit Throw instead of a bare Stop. Write-then-raise is the shape the relaxation makes authorable. | legacy-oracle parity capture (placement only) |
| `connector_linear_interleave` | 5 | **SPLIT — corrected by QA-154-r1-08.** The case has two halves and they do NOT have the same standing. The INTERLEAVE half (linear steps *between* two calls) **is** legacy-attested: `tests/fixtures/golden_xml/set_properties_ddp_dpp_flow_sequence.xml`, committed 2026-07-02 and therefore long before this slice's baseline, emits `[start, connectoraction, documentproperties, documentproperties, connectoraction, stop]` — measured. The PREFIX half (a linear step *before the first* call) is genuinely unattested: no pre-baseline golden places a non-`catcherrors` shape ahead of the first connector shape. The first draft of this row claimed no oracle for either half, which UNDERSTATED the evidence — a provenance record is wrong when it is too pessimistic as well as when it is too generous. | interleave: legacy-oracle parity capture · prefix: UNATTESTED, recorded |

## The one placement this slice does not attest

The linear PREFIX of `connector_linear_interleave` — a property step ahead of the flow's first
connector call — has no independent oracle at this HEAD. What stands behind it:

* the entry role is proven to sit on the first ROOT CALL rather than on the CFG entry node
  (`tests/test_process_ir_compiler_invariants.py`, three mutants, each asserting the mutation took
  effect first);
* `verify_process_graph` accepts the emitted XML and the platform's own readback graph-verifies
  clean;
* the bytes are deterministic across isolated renders.

What is NOT established is that the platform renders a source-role connector shape at a non-zero
position the way this emission assumes. A UI-authored reference component, or an executed-green
live capture, would close it. Recorded as a limitation rather than argued away.

## Why `error_symbols`

Every case reuses `_wave_gate_golden_corpus.error_symbols()`. That table already models a REST
GET/PATCH pair, a database Send, a connection, two profiles and a document cache — which is exactly
the vocabulary these six shapes need. Adding a parallel table would be a second hand-model of the
same facts.
