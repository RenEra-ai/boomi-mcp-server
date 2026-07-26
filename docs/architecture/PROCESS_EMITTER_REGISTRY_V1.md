# Process-Emitter Registry V1 (issue #138, M12.3)

Status: **DARK, with the first PRODUCTION consumers as of #139A (2026-07-22).** The registry is the
consumer of `EmissionPlanV1`. It lives at `src/boomi_mcp/compiler/process_ir/emitter_registry.py`, is
imported directly (never through `boomi_mcp.compiler.process_ir.__all__`), and stays out of every MCP
tool / JSON Schema. Since #139A its `emit_process` is driven in production by the legacy adapters
(`legacy_adapters/emission.py`) for the migrated `wrapper_subprocess` and
`database_to_api_sync/flow_sequence` build paths: `emit_process` returns the ordered shape fragments +
the minimal verified `<process>` wrapper, and the **legacy component assembler
(`_assemble_process_component_xml`) remains the owner of the deployable Boomi Component envelope**
(description, folder, `processOverrides`, the `<bns:Component>` skeleton). #139 later slices bring the
remaining dialects; see the [compatibility inventory](M12_COMPATIBILITY_INVENTORY.md) #139 ledger.

This milestone is **mechanical consolidation only**: no new ProcessIR semantic nodes, no
connector/control-flow capability, no public schema change. It extracts the byte-proven shape
serializers out of `ProcessFlowBuilder` so both the legacy builder and the typed registry emit
from **one copy**.

## 1. Where the code lives

```
src/boomi_mcp/categories/components/builders/process_emitters/
  rendering.py   # the SINGLE copy of every shape XML template + layout primitives + value objects
  legacy.py      # adapters preserving the historical `_emit_*` dict signatures/behavior
src/boomi_mcp/compiler/process_ir/
  emitter_registry.py   # the typed registry: emit_process(plan, symbols) -> artifact
```

`process_flow_builder.py` keeps all orchestration (config-kind dispatch, topology/Try-Catch
composition, `_emit_flow_shape`, `_emit_*_shapes`, the component-envelope assembly) and imports its
leaf emitters from `process_emitters.legacy` — it owns **no** XML template. `rendering.py` imports
nothing from `process_flow_builder` (that would close an import cycle); the emission-domain
constants it needs live in `rendering.py` and the builder imports them back.

## 2. The registry contract

```python
def emit_process(
    emission_plan: EmissionPlanV1,
    resolved_symbols: SymbolTableV1,
    *,
    capability_level: CapabilityLevel = CAPABILITY_PROCESS_IR_V1,
) -> ProcessEmissionArtifactV1
```

`ProcessEmissionArtifactV1` carries the ordered `shape_xml_parts`, the minimal
`<process xmlns=""><shapes>…</shapes></process>` they were verified in, and a
`ProcessVerifierSummaryV1`. It is **not** a deployable component envelope.

Each `EmitterRegistration` declares: the accepted `emitter_kind`, the typed input class, the
produced Boomi shape type, the supported capability level, the allowed outgoing cardinality, a
symbol-requirement function, an emit callable, and an optional precondition. The `emit` callable
takes **only** a typed `EmitterInputV1` member and an `EmitterContext` (the plan node + the symbols
it declared it needs) — never a raw `IntegrationSpecV1`/`PipelineSpec`/legacy builder config, and no
facility to mutate anything (guarded by `test_process_emitter_registry.py`).

## 3. The closed manifest — 18 keys, 17 model classes

Registry completeness is validated at import against
`TypeAdapter(EmitterInputV1).json_schema()`'s discriminator mapping (`_validate_coverage`). The 17
model classes yield 18 discriminator keys because `ConnectorActionInputV1` accepts both
`connectoraction_source` and `connectoraction_target` — the two keys share one connector renderer.

| Registry key | Input model | Boomi shape | Outgoing | Required symbols |
|---|---|---|---|---|
| `start_noaction` | `StartNoActionInputV1` | `start` | 1 | — |
| `connectoraction_source` | `ConnectorActionInputV1` | `connectoraction` | 1 | connection=`connector-settings`, operation=`connector-action` |
| `connectoraction_target` | `ConnectorActionInputV1` | `connectoraction` | 1 | connection=`connector-settings`, operation=`connector-action` |
| `message` | `MessageInputV1` | `message` | 1 | — |
| `map` | `MapInputV1` | `map` | 1 | map=`transform.map` |
| `flowcontrol` | `FlowControlInputV1` | `flowcontrol` | 1 | — |
| `dataprocess` | `DataProcessInputV1` | `dataprocess` | 1 | each split/combine profile ∈ {`profile.json`,`profile.xml`} |
| `doccacheload` | `DocCacheLoadInputV1` | `doccacheload` | 0 or 1 | cache=`documentcache` |
| `doccacheretrieve` | `DocCacheRetrieveInputV1` | `doccacheretrieve` | 1 | cache=`documentcache` |
| `doccacheremove` | `DocCacheRemoveInputV1` | `doccacheremove` | 1 | cache=`documentcache` |
| `setproperties_step` | `SetPropertiesStepInputV1` | `documentproperties` | 1 | each profile source ∈ {`profile.db`,`profile.json`,`profile.xml`} |
| `processcall` | `ProcessCallInputV1` | `processcall` | 1 | process=`process` |
| `branch` | `BranchInputV1` | `branch` | `num_branches` | — |
| `decision` | `DecisionInputV1` | `decision` | 2 | — |
| `exception` | `ExceptionInputV1` | `exception` | 0 | — |
| `stop` | `StopInputV1` | `stop` | 0 | — |
| `returndocuments` | `ReturnDocumentsInputV1` | `returndocuments` | 0 | — |

## 4. Two-pass, fail-closed emission

1. **Preflight every node** (before any XML is produced): registration exists at the current
   capability; the typed input matches the registration's `input_type`; the renderer precondition
   holds; the outgoing cardinality matches; every required component id resolves to a symbol of an
   accepted type. Diagnostics accumulate across all nodes.
2. If any node failed → raise one `ProcessIRCompileError` with the accumulated, canonically-sorted,
   **value-free** diagnostics. **No partial output escapes.**
3. Otherwise emit each shape **in `EmissionPlanV1.nodes` order** (never registry order), wrap the
   parts, parse-back and cross-check the shape count/name/type against the plan, then run
   `verify_process_graph`. Verifier **errors** are a compilation failure; **warnings** are returned
   unchanged in the artifact.

The registry does **not** re-run `check_emission_plan_invariants` (that stays the CFG-aware compiler
check); it adds only the local readiness checks possible from `EmissionPlanV1` + `SymbolTableV1`.

### Diagnostic mapping

| Condition | Code | Phase |
|---|---|---|
| Unknown kind / missing registration / capability too low | `PROCESS_IR_COMPILE_EMITTER_MISSING` | `xml_emission` |
| Input/discriminator mismatch, bad precondition, or cardinality mismatch | `PROCESS_IR_COMPILE_EMITTER_INPUT_INVALID` | `xml_emission` |
| Required component id absent or only present with an incompatible type | `PROCESS_IR_COMPILE_SYMBOL_UNRESOLVED` | `reference_resolution` |
| Malformed XML or shape count/name/type disagreement | `PROCESS_IR_COMPILE_XML_INVALID` | `xml_emission` |
| Graph verifier returned errors | `PROCESS_IR_COMPILE_VERIFIER_FAILED` | `post_emission_verification` |
| Unexpected renderer exception | `PROCESS_IR_COMPILE_INTERNAL` (reused from #137) | `xml_emission` |

`xml_emission` (rank 4) and `post_emission_verification` (rank 5) are the two new `CompilerPhase`
values, ranked after `emission_planning`. **Security:** every message and remediation is a static,
value-free string — no component id, symbol ref, input payload, XML fragment, verifier payload, or
caught exception text is ever interpolated.

## 5. Determinism and byte-preservation rules

- **Geometry is consumed verbatim** from the plan: shape ids, coordinates, dragpoints and synthetic
  nodes come from `EmissionNodeV1.layout`/`.shape_id`/`.outgoing`. The registry recomputes nothing —
  no mutable shape/branch/step counter, no coordinate calculator.
- Emit order is `plan.nodes` order; the registry mapping order never reaches output.
- The renderers never reserialize through `ElementTree`; they build strings. Load-bearing details:
  attribute order, `x="96.0"` float `.0` rendering, self-closing spellings (`<parameters/>`,
  `<dragpoints/>`), child order, MessageFormat apostrophe escaping/JSON single-quote wrapping, the
  decision `identifier="true"`/`text="True"` case asymmetry, and the absence of a trailing newline.

## 6. Legacy-only exceptions (NOT registry kinds)

| Legacy behavior | Why not registered | Owner |
|---|---|---|
| `_emit_start_listen` (WSS Listen start) | WSS metadata fused into the Start shape; no current input kind | #140 |
| `_emit_notify` | absent from current ProcessIR | #142 |
| connector `dynamic_path` form | absent from `ConnectorActionInputV1` | #139/#140 |
| catch-row ProcessCall variants | legacy compositions sharing registered serializers | #142 |
| listener/process options, overrides, component envelope | artifact metadata, not plan nodes | #139 |
| `emit_fragment` | intentionally free-form; never a canonical ProcessIR node | permanently excluded |
| `route` | verifier recognizes it; no current builder emitter exists | no entry |
| "synthetic transport shape" | no such shape is emitted — lowering wires connector→map directly | documented absent |

## 7. Parity evidence

Byte parity is the hard gate. Two layers of oracle:

- **Existing raw-byte goldens** in `tests/fixtures/golden_xml/` (converted from C14N to raw `==` in
  #138), plus five new frozen fixtures for previously structural-only or differential-only paths
  (`listener_wss_start.xml`, `dynamic_path_target_profile.xml`, `dynamic_path_source_ddp.xml`,
  `try_catch_dlq_error_subprocess.xml` — the catch-row error-subprocess ProcessCall variant, and
  `sync_pipeline_db_read_map_rest_send.xml` — a direct byte anchor for a non-listener SyncPipeline
  build). No existing golden's committed bytes changed.
- **`tests/test_process_emitter_parity.py`**: for the three golden IR documents (`process_ir_v1.json`
  — `control_flow`, `linear_flow`, `wrapper_flow`, which together exercise **all 17** emitter kinds),
  the registry's `emit_process` shapes equal the UNCHANGED legacy builder's `<shapes>` byte-for-byte,
  equal a committed pre-extraction fixture (`tests/fixtures/process_ir/emitter_parity/*.process.xml`),
  and produce a verifier summary matching `verify_process_graph` on the legacy XML.

`tests/test_process_emitter_registry.py` covers the contract: completeness vs the discriminator,
duplicate-key rejection, unknown/missing emitter, missing and wrong-type symbols, duplicate
component ids with one compatible alias, cardinality, whole-plan preflight (a bad later node blocks
ALL rendering), determinism, symbol-order and registry-order invariance, renderer-exception →
`PROCESS_IR_COMPILE_INTERNAL`, the absence of `emit_fragment`/`start_listen`/`notify`/`route`
(and, since #142, the PRESENCE of `catcherrors`), and the AST/import isolation guard.

## 8. #140 M12.5 — ConnectorCall adds NO registry key

The registry stays at exactly **17 discriminator keys / 16 model classes**. `ConnectorCallNodeV1`
(#140) reuses the two existing connector registrations rather than introducing a third:

| ConnectorCall placement | emitter key | renderer |
|---|---|---|
| the entry call (first step of the sequence) | `connectoraction_source` | `render_connectoraction` |
| every later call | `connectoraction_target` | `render_connectoraction` (the same function) |

This is sound because the two keys were already degenerate at the XML layer: they share one renderer,
and since #139C the connector canonicalization rule is **family-conditional and role-independent**, so
selecting between them changes the plan's discriminator and not a byte of output. The placement is
derived by the compiler from the call's authored position — a caller authors no role.

Nothing else in this document changes: no renderer, no attribute, no image name, no cardinality rule,
and no `parameter-profile`/`dynamicProperties` behaviour. #140 enables only the **simple** connector
binding, so a `connector_call` always renders the `<parameters/>` + empty `<dynamicProperties/>` form —
the same form the shipped, live-QA-verified `sync_pipeline_*` goldens carry. Dynamic paths and
parameter binding remain out of the capability allowlist.

The new fixture `tests/fixtures/process_ir/emitter_parity/connector_call_mixed.process.xml` is the
first golden with **no legacy oracle** (the legacy builder cannot express a multi-connector flow); the
four checks that stand in for one are listed in
[PROCESS_IR_COMPILER_V1 §10a](PROCESS_IR_COMPILER_V1.md).

## #141 M12.6 — rich control bodies need no new emitter

Branch and Decision keep their existing registrations (`BRANCH` arity, `EXACT_TWO` arity) and their
existing renderers. A rich body changes only which CFG node each control transition targets, and the
emission plan already carries that. `numBranches` is derived from the authored leg count and must
equal the wired transition count; a mismatch — like every other control-wiring defect — now raises
`PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID` rather than the generic emission-plan code.

The graph verifier's bare-Stop finding stays a **warning**, not an error: a Decision routing its
false outcome straight to a Stop is live-attested production shape, so treating intentional document
dropping as a failure would reject valid processes.


## #142 M12.7 — `catcherrors` becomes registry key 18

Scoped Try/Catch is now a compiled ProcessIR construct, so the shape has a registry key:

| Field | Value |
|---|---|
| discriminator | `catcherrors` |
| input model | `CatchErrorsInputV1` (one field: `retry_count`, bounded 0–5) |
| produced shape type | `catcherrors` |
| outgoing cardinality | `EXACT_TWO` — Try then Catch |
| capability | `PROCESS_IR_V1` |

`catcherrors` moves **out** of §6's legacy-only list. It was there because no compiled construct
produced it; that is no longer true.

**No byte moved.** The adapter calls the *shared* `rendering.render_catcherrors` — the same function
the legacy adapters already used — rather than importing or duplicating a private legacy emitter, so
there is exactly one definition of this shape's bytes and all pre-existing Try/Catch goldens are
unchanged. `render_catcherrors`'s docstring lost its "(legacy-only)" note; its output did not change.

Two settings stay fixed inside the renderer rather than becoming input fields: the all-errors flag
and the userlabel that describes it. They are locked together, and both callers depend on the
current text — see PROCESS_IR_V1 §3c for why that is recorded as a deliberate surface omission
rather than an unknown.

The precondition `_pre_catcherrors` re-checks the 0–5 bound. `CatchErrorsInputV1` already enforces
it, so a plan built through lowering can never fail there; the precondition exists for the other
caller — a hand-built plan handed straight to the registry, which the typed model never saw.

Listener start, Notify, Route and `emit_fragment` remain absent.
