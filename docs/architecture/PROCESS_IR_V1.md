# ProcessIRV1 — Strict Semantic Process Models (issue #136, M12.1)

**Status:** shipped dark; the FIRST production cutover landed in #139A (2026-07-22). The models exist
(`src/boomi_mcp/models/process_ir.py`) and are exported from `boomi_mcp.models`; there is **no direct
ProcessIR authoring surface** — a caller never hand-writes IR. #136 added no MCP surface. **Production
ingress now exists** for two legacy dialects: `wrapper_subprocess` and the composed
`database_to_api_sync/flow_sequence` are normalized by the legacy adapters
(`src/boomi_mcp/compiler/process_ir/legacy_adapters/`, #139A) into `ProcessIRV1` and then
`compile_process_ir_v1 → emit_process`, replacing their pre-#139 per-dialect XML orchestration
byte-identically. The **compatibility projection** the adapter applies: an already-validated legacy
config's currently-executed fields feed the IR; safe unknown root/binding keys are recorded as
`compatibility_noop_paths` (never rejected); connector/component references become symbol requirements
(connector metadata rides on the operation symbol, ADR-001 §6); envelope data (description, folder,
process_extensions) stays OUTSIDE the IR, owned by the component assembler. Direct ProcessIR authoring
stays dark. Every OTHER legacy dialect (ordinary `database_to_api_sync`, `sync_pipeline`, listeners,
recipes) continues through its unchanged path until a later #139 slice cuts it over — see the
[compatibility inventory](M12_COMPATIBILITY_INVENTORY.md) #139 ledger.
**References:** [ADR-001](ADR-001-process-ir-authority.md) (authority model, §7 error families,
§11 security), [ProcessIR Compiler V1](PROCESS_IR_COMPILER_V1.md) (the #137 CFG/lowering
contracts consuming these models), [M12 Compatibility Inventory](M12_COMPATIBILITY_INVENTORY.md)
(frozen baseline), issue #136 / epic #134.

ProcessIRV1 is the **promotion of the frozen `flow_sequence` vocabulary** into a strict,
versioned, discriminated model family (ADR-001 §12: it is a successor and normalization, not a
parallel fourth DSL). Current-parity nodes represent exactly what today's builder can execute;
everything richer is a **later M12 capability addition** (see the capability table below).

## 1. Document shape and versioning

```json
{"version": "1", "body": {"kind": "sequence", "steps": [ ... ]}}
```

- `ProcessIRV1(version="1", body=SequenceNodeV1(...))` is the semantic root — exactly one per
  authored process (ADR-001 §3).
- Every authored boundary is `extra="forbid"`; unknown fields are rejected, never dropped.
- Any semantic change to an accepted document requires a **new IR version** (ADR-001 §9);
  version `"1"` is never mutated in place. An unsupported/missing `version` fails with
  `PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED` before model validation.
- Entry point: `parse_process_ir_v1(payload)` — gate order: payload shape → secret scan →
  version → strict model validation.

## 2. Authored vs derived (ADR-001 §6)

Callers author exactly **two things**: semantic nodes and **opaque component references**.

- `ComponentRefV1` accepts an exact `$ref:KEY` token (byte-0 prefix, no surrounding
  whitespace, non-empty whitespace-free key) or a literal component id. Violations fail with
  `PROCESS_IR_REFERENCE_INVALID_FORMAT`.
- The IR carries **no** connector family, HTTP action, profile metadata, CFG edges, shape or
  layout identifiers, XML fragments, coordinates, or free-form `config` dictionaries. Connector
  metadata (`connector_type`/`action_type`) is **derived** by the compiler from its
  symbol-table resolution context — never authored, never serialized in IR JSON.
- Generated Boomi ids for shapes/layout cannot be expressed; component ids appear only as
  opaque references.

## 3. Node inventory (current parity)

Root sequence (`SequenceNodeV1.steps`, discriminated on `kind`):

| Kind | Model | Notes / defaults (all grounded in the frozen builder grammar) |
|---|---|---|
| `source` | `SourceEndpointV1` | `connection_ref`, `operation_ref`, optional `label`; first step of a connector flow |
| `target` | `TargetEndpointV1` | same fields; success terminal position only |
| `flow_control` | `FlowControlNodeV1` | `for_each_count` strict int > 0 |
| `message` | `MessageNodeV1` | `text` non-empty |
| `map_ref` | `MapRefNodeV1` | `map_ref` component reference |
| `data_process` | `DataProcessNodeV1` | 1+ ops: `custom_scripting` (`language="groovy2"`, `use_cache=True`), `split_documents`, `combine_documents` (`combine_into_link_element_key="null"`) |
| `cache_put` | `CachePutNodeV1` | Add to Cache; consumes the stream (see sequence rules) |
| `document_cache_retrieve` | `DocumentCacheRetrieveNodeV1` | `empty_cache_behavior="stopprocess"`, `load_all_documents=True` (legacy M10.5 retrieve) |
| `cache_get` | `CacheGetNodeV1` | `empty_cache_behavior="stopprocess"`, `external_writer=False` (authored lineage assertion) — kept distinct from `document_cache_retrieve` |
| `cache_remove` | `CacheRemoveNodeV1` | `remove_all_documents=True` |
| `set_ddp` / `set_dpp` | `SetDdpNodeV1` / `SetDppNodeV1` | bare `name` (no wire prefix, no whitespace), ordered `source_values` (static/current/profile/ddp/dpp); DPP adds `persist=False` |
| `process_call` | `ProcessCallNodeV1` | `process_ref`, `wait=True`, `abort_on_error=False`, optional `label` (wrapper parity) |
| `branch` | `BranchNodeV1` | 2–25 `BranchLegV1` legs: linear `steps` + terminal `target` or target-less staging `cache_put` |
| `decision` | `DecisionNodeV1` | 7 comparisons; `track`/`static` operands; typed `true_arm` (terminal target/branch/exception) and `false_arm` (terminal stop/branch/exception); nested decision is impossible by schema |
| `exception` | `ExceptionNodeV1` | `message_template` (needs `{1}` unless `parameter_source="none"`), optional `title`, `stop_single_document=False`, `parameter_source="caught_error"`; **no `label`** (legacy parity) |
| `stop` | `StopNodeV1` | no fields (continue semantics are emitter-owned) |
| `return_documents` | `ReturnDocumentsNodeV1` | optional `label` |

Sequence rules (local/structural — the CFG-aware checks are #137/#143):

- A **connector flow** starts with `source` and ends in exactly one of `target`+`stop`, a
  **standalone** `return_documents` terminal, or a terminal control
  (`branch`/`decision`/`exception`). Controls and terminals may appear only in the final
  position. The Return Documents terminal is standalone because the legacy builder emits
  ONLY `returndocuments` after the sequence when `return_documents` is enabled — the
  configured legacy root target is dead config and is not represented in IR.
- A **process-call flow** contains only `process_call` steps plus a `stop`/`return_documents`
  terminal; mixing connector nodes with process calls is capability-gated
  (`mixed_connector_execution`).
- `cache_put` must be immediately followed by a stream-replacing cache read
  (`cache_get`/`document_cache_retrieve`); a trailing `cache_put` in a branch leg is expressed
  as the leg's staging **terminal**, and a decision false-arm may end its steps with
  `cache_put` only before a `stop` terminal (all legacy consume-guard parity).
- Branch/Decision **terminalize** their sequence (no continuation after them — gated for #141).

## 4. Alias normalization (private codec)

The public model has ONE canonical spelling per node. Legacy spellings are normalized only in
the private `_process_ir_compat` codec (unexported, test-only; #139 may absorb it, and its
compatibility-only machinery — e.g. the `fallback_target` reconstruction of the
legacy-required-but-unemitted root target — carries an M12 removal gate in #147):

| Legacy | IR |
|---|---|
| `dataprocess` | `data_process` |
| `doccacheload` | `cache_put` |
| `doccacheretrieve` | `document_cache_retrieve` |
| `doccacheremove` | `cache_remove` |

The codec's equivalence contract is **canonical-IR equality** (`canonical(legacy→IR) ==
canonical(legacy→IR→legacy→IR)`) with defaults expanded — legacy spelling identity is
explicitly not a goal. The decision true-arm target is the hoisted legacy root target (its
emitted fallthrough); a root target made dead by a branch/exception/return_documents
terminal is not represented in IR (the codec re-synthesizes it from `fallback_target` on
the reverse path). Legacy endpoint `label`s ride through both directions.

## 5. Canonical serialization and goldens

`canonical_process_ir_json(ir)`: `model_dump(mode="json")` (defaults and `None`s included) then
`json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=True)`; list order preserved.
`canonical_process_ir_schema_json()` serializes `ProcessIRV1.model_json_schema()` the same way.
Committed goldens: `tests/fixtures/process_ir/process_ir_v1.json` (three full-vocabulary
documents) and `process_ir_v1.schema.json`, pinned byte-equal twice per test run.

**Golden regeneration:** the schema golden is pinned to the current pydantic (2.12.x). A
pydantic upgrade that changes schema output forces a **reviewed** regeneration of
`process_ir_v1.schema.json` — never a silent refresh.

## 6. Diagnostics (ADR-001 §7)

`parse_process_ir_v1` raises `ProcessIRValidationError` carrying `ProcessIRDiagnostic` entries
sorted by `(path, code)`:

| Code | Meaning |
|---|---|
| `PROCESS_IR_SCHEMA_UNKNOWN_NODE` | unknown `kind`/discriminator tag |
| `PROCESS_IR_SCHEMA_UNKNOWN_FIELD` | extra field on a strict node |
| `PROCESS_IR_SCHEMA_INVALID_CARDINALITY` | list bound / ordering / terminal rule violated |
| `PROCESS_IR_SCHEMA_VERSION_UNSUPPORTED` | missing/unsupported `version` |
| `PROCESS_IR_SCHEMA_INVALID` | any other strict-schema mismatch |
| `PROCESS_IR_REFERENCE_INVALID_FORMAT` | malformed opaque reference |
| `PROCESS_IR_CAPABILITY_UNSUPPORTED` | gated/unsupported construct (keyed cache, `definedparameter`, secret carriage, process-call mixing) |

Every diagnostic carries a stable code, an RFC 6901 JSON pointer into the **authored** payload,
and static remediation text. Raw Pydantic `input`/`ctx` values are never propagated; messages
never echo authored values. The code constants live in the shared `boomi_mcp.errors` registry
(ADR-001 §7: one registry per family; #140–#143 add codes, never rename these).

## 7. Security (ADR-001 §11)

- A pre-parse scan rejects secret-shaped keys (same substring semantics as the builders'
  `FORBIDDEN_SECRET_FIELDS`, pinned equal by test) with `PROCESS_IR_CAPABILITY_UNSUPPORTED`,
  naming the JSON path but **never the value**.
- Model `repr`/`str` suppress every authored value (only discriminators and the version render).
- Fixtures and goldens use sentinel refs/values only.

## 8. Capability states

Published as the immutable `PROCESS_IR_V1_CAPABILITIES` manifest (not an authored field):

| Capability | State | Owner |
|---|---|---|
| generalized ConnectorCall, mixed connector execution | gated | #140 |
| continuation after Branch/Decision, rich bodies | gated | #141 |
| scoped Try/Catch | gated | #142 |
| keyed cache (`doc_cache_index`/`cache_key_values`/keyed `load_all_documents`) | gated | no live-captured wire shape (#119) |
| `definedparameter` property source | gated | no verified wire shape |
| joins, loops | gated | ADR-001 §8 |
| caller-authored CFG edges, XML/layout/shape ids, secret values | unsupported (permanent) | ADR-001 §12 |

## 9. Ownership boundaries (#137–#143)

- **#137** owns the compiler CFG + lowering contracts consuming these models (shipped dark —
  see [PROCESS_IR_COMPILER_V1](PROCESS_IR_COMPILER_V1.md); it adds the `PROCESS_IR_SEMANTIC_*`
  and `PROCESS_IR_COMPILE_*` families and rejects listener entry with #136's
  `PROCESS_IR_CAPABILITY_UNSUPPORTED` until #140); **#138** the
  verified emitter registry; **#139** the production legacy adapters (including the legacy
  config-root leniency — inventory §2.7 — which #136 deliberately does NOT tighten); **#141/#142**
  the gated control-flow/error-handling capabilities; **#143** CFG-aware semantic validation.
- Strictness applies to the **new** IR surface only. No existing request contract is removed,
  reinterpreted, or tightened by #136; the #135 freeze suite runs unchanged.

## #138 M12.3 update — EmissionPlanV1 has a verified internal consumer

`EmissionPlanV1` now has a **test-only** consumer: the process-emitter registry (#138,
`compiler/process_ir/emitter_registry.py`, `emit_process`). It reuses the byte-proven shape
serializers extracted into `process_emitters/` and emits XML byte-identical to the legacy builder for
all 17 emitter kinds. There is still **no MCP/runtime adapter** — the registry is imported directly,
never exported, and invoked by no tool or production builder; #139 owns the production cutover. See
`docs/architecture/PROCESS_EMITTER_REGISTRY_V1.md`.
