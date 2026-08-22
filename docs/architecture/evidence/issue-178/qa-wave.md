# Stage-1 QA — WAVE-GATE live component, issue #178

Attested `98506189fd3d2ea8e37b3dc2464233f70c82d87c`. Tree `clean` at PRE and `worktree_moved=False`
at ATTEST on every run; repo untouched; account back to 22 / 24 / 22 with licences unchanged
(standard 3/0, standard_test 3/0).

**This is not another fix-delta round.** The wave gate asks for one live scenario per changed
CAPABILITY CLASS across the whole slice. The eight previous rounds were delta-scoped by construction
and cannot answer a whole-slice question, so they were not treated as a substitute.

## Capability classes — enumerated by QA, not supplied

QA enumerated the 8 changed functions from the delta and bound them to public arms BY MEASUREMENT,
running a pipeline census and a new models-layer census under both live matrices. That mattered: the
models-layer change has a WIDER blast radius than the compile entry, naming six consuming layers the
delta rounds never bound — including `models/authoring_workflow.py::parse_authoring_request_v1` (the
MCP intake parse) and `models/_process_ir_compat.py::legacy_flow_sequence_to_ir`.

| # | Capability class | Live scenario | Result |
| --- | --- | --- | --- |
| C1 | Typed ProcessIR plan/compile/apply | apply matrix `D_*` | GREEN |
| C2 | Typed ProcessIR EXECUTION | package -> deploy -> execute -> record | **COMPLETE, 0 error docs** |
| C3 | Served parser diagnostics at the boundary | catch-terminal N1–N7 | GREEN |
| C4 | Legacy `wrapper_subprocess` (policy-bearing) | `E1_apply` | GREEN |
| C5 | Legacy `sync_pipeline` (policy-bearing) | `E4_apply` | GREEN |
| C6 | Legacy `flow_sequence` | `E2`/`E3` apply + compose | GREEN |
| C7 | Archetype builds / recipe-engine compile | 6 archetypes + 2 compose oracles | GREEN |
| C8 | Revision binding / canonical JSON | plan+compile binding, canonical equivalence | GREEN 6/6 |
| C9 | Mutation accounting on refusal | `B1`/`B2` + positive control | GREEN |
| C10 | Direct `intent_kind=recipe` arm | 8 registered recipe ids driven | **GAP** |

The enumeration corrected the implementer's: `flow_sequence` is a distinct legacy dialect reaching
the changed parser, and it deserved naming even though it is covered.

## Execution truth — the evidence no delta round could produce

Seven rounds proved the slice emits the right bytes. None proved they RUN. Four attempts were needed,
each refusal naming its own fix:

```
process   _TEST_QA178wz…_z   (typed-authored root, XML digest 82f4f599f4665d96)
status    COMPLETE           execution_duration 29s
inbound_document_count 1     outbound 0     inbound_error_document_count 0
atom      renera-local-atom
```

A runtime control (`_QA_FIXTURE_noop` -> COMPLETE) ran FIRST, so a failure could not have been
confused with a sick atom.

## Standing regression, eighth round

Five emitted-XML digests exact, both frozen 2026-07-11 M8 oracles byte-for-byte, E-matrix leaf-diff
one stamp, `B1`/`B2` inventory 22 -> 22 with `mutation_performed:false` against five real creates.

## Findings

- **`QA-178-wave-01`** — `orchestrate_deploy` deploys the wrong component on a typed-authoring build
  and reports success: given a spec with a `reference_only` child it resolved to the CHILD, deployed
  that, and returned `_success: true` with `total_components: 1` listing only the reused child. The
  authored root was never deployed, so `execute_process` refused it. Pre-existing, out of slice.
- **`QA-178-wave-02`** — QA changed shared account state: that call left `_QA_FIXTURE_noop` at
  version 3. Disclosed unprompted; no licence consumed; re-verified COMPLETE afterwards.
- **`QA-178-wave-03`** — C10 gap. All 8 registered descriptors refuse every constructible input;
  `SyncRecipeInputV1` / `ComposeDbRestFanoutInputV1` need `component_slots`/`targets` resolving
  against `base_components`. Bounded: the recipe engine's CHANGED code is covered via
  `compose_archetypes` and `build_from_archetype`; only the MCP arm reaching a successful recipe run
  is unreached. Not a missing fixture and not a licence ceiling.

## Where this round is not discriminating

The live sweep protects the shared path but says nothing about the forgery class — unreachable from
MCP for the eighth round running. C3 is the only boundary-observable behaviour change in the entire
slice. The genuinely new evidence is the execution truth and the capability-class binding.
