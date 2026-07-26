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
process_extensions) stays OUTSIDE the IR, owned by the component assembler. Since **#139B** the flow
adapter's IR uses internal OCCURRENCE-SCOPED aliases (`$ref:legacy.adapter:<RFC6901-pointer>`, no
authored value) in place of raw component ids — each requirement carries a `legacy_selector` that
resolves through `SymbolTableV1` to the real component id before emission, so aliases NEVER appear in
XML and one id reused across roles keeps a distinct symbol per occurrence. Direct codec output (and
the frozen #136 equivalence contract) still contains authored raw refs — only the production adapter
aliases; all public `ComponentRefV1` semantics are unchanged. Direct ProcessIR authoring stays dark.
Every OTHER legacy dialect (ordinary `database_to_api_sync`, `sync_pipeline`, listeners, recipes)
continues through its unchanged path until a later #139 slice cuts it over — see the
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
| `connector_call` | `ConnectorCallNodeV1` | **#140**: `operation_ref` + optional `action` assertion + optional `label`. No `connection_ref` — see below |
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
| `branch` | `BranchNodeV1` | 2–25 `BranchLegV1` legs; **#141** rich bodies — see §3b |
| `decision` | `DecisionNodeV1` | 7 comparisons; `track`/`static` operands; typed `true_arm`/`false_arm`; **#141** rich bodies incl. nested decision — see §3b |
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
  (`process_call_connector_mixing` — renamed from `mixed_connector_execution` by
  #140, which took that name back for its own, ADR-001 §8 meaning).
- `cache_put` must be immediately followed by a stream-replacing cache read
  (`cache_get`/`document_cache_retrieve`); a trailing `cache_put` in a branch leg is expressed
  as the leg's staging **terminal**, and a decision false-arm may end its steps with
  `cache_put` only before a `stop` terminal (all legacy consume-guard parity).
- Branch/Decision **terminalize** their sequence. A node authored after one fails with
  `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` (#141);
  `continuation_after_branch_or_decision` stays gated.
- A **control-only** root (#141) is exactly one `branch`/`decision` and nothing else. It models the
  live `start -> branch` shape, and it is what makes a ProcessCall-only path's root-to-leaf path
  genuinely connector-free.
- A **connector-call flow** may now also terminate in `branch`/`decision` (#141), not only
  `stop`/`return_documents`. `exception` is deliberately not admitted there. A `map_ref` still may
  not directly precede the control — its target profile would have nothing in the root body to be
  checked against, which is the continuity hole map bracketing exists to close.
- A **connector-call flow** (#140) is a third, mutually exclusive sequence mode: it starts with a
  `connector_call`, contains only `connector_call` and `map_ref` steps, and ends in `stop` or
  `return_documents`. Every `map_ref` must be immediately followed by a `connector_call` (so each map
  is *bracketed* by calls, which is what makes the compiler's profile-continuity check total). Mixing
  it with `source`/`target` or `process_call` is rejected. Legacy `source`/`target` and `process_call`
  sequences take exactly the paths they did before #140.

### 3a. `connector_call` — the #140 node

```json
{"kind": "connector_call", "operation_ref": "$ref:op_rest_get", "action": "GET", "label": "Read orders"}
```

- **`operation_ref` is the only reference authored.** `connection_ref` is deliberately absent: no
  connector-action component declares its own connection (live capture, `.codex/plans/issue-140-live-captures.md`
  FINDING 1; the repo's own operation builders state "Boomi binds the connection at the process
  connector step, not in the operation XML"), so the operation→connection edge is a fact of the
  **component plan**. The compiler receives it as resolution context on
  `ComponentSymbolV1.connection_ref` (ADR-001 §6). Authoring it in IR would recreate the
  duplicate-authority split ADR-001 exists to remove — and `extra="forbid"` rejects the attempt.
- **`action` is an assertion, never an override.** The family and action that reach the wire always
  come from the resolved operation symbol. A supplied `action` is compared case-insensitively against
  the authoritative one and can only ever *reject*; omitting it is fully supported.
- Multiple calls may appear anywhere in the sequence, in any supported family. Which call becomes the
  `connectoraction_source` shape and which become `connectoraction_target` shapes is **derived from
  position**, never authored.
- Everything else stays forbidden by the existing strictness: no config dict, XML, layout, shape id,
  CFG edge, connector family, profile, credential, or header.

### 3b. Rich Branch/Decision bodies — the #141 body matrix

A Branch leg / Decision arm is `steps` + one `terminal`. Which node kinds each slot admits is a
**closed allowlist**, published as data in `compiler/process_ir/body_capabilities.py` and pinned
against the model unions in both directions. **Absence is denial** — there is no wildcard and no
"known kind" fallback, so every future node kind is rejected until a row is deliberately added.

| Slot | Admitted kinds |
|---|---|
| `branch_leg.step` | the linear set, `connector_call`, `process_call` |
| `branch_leg.terminal` | `target`, `cache_put`, `stop`, `decision` |
| `decision_true_arm.step` | the linear set, `connector_call`, `process_call` |
| `decision_true_arm.terminal` | `target`, `stop`, `exception`, `branch`, `decision` |
| `decision_false_arm.step` | the linear set, `connector_call` |
| `decision_false_arm.terminal` | `stop`, `exception`, `branch`, `decision` |

Every admitted placement is live-attested in `.codex/plans/issue-141-live-captures.md`.
Deliberate exclusions, all fail-closed on absent evidence: **`branch` as a Branch-leg terminal**
(nested control in a leg is attested only as a *Decision*), **`process_call` on a FALSE arm**
(attested on TRUE outcomes only), and `return_documents` anywhere in a body.

**ProcessCall path mode.** A body that uses `process_call` may contain nothing else and must end in
`stop`, **and no connector may run anywhere upstream on its root-to-leaf path**. Both halves are
required: the body-local rule keeps the body connector-free, but a body cannot see its ancestors.
Checking only the ROOT is not enough either — a connector can sit in an outer control body while the
`process_call` sits in a nested one, both on one path — so the document is walked carrying whether a
connector has run above. That is what makes `process_call_connector_mixing` honestly gated at every
depth rather than only the shallowest; sibling legs are independent paths, not a mix.

**Bare Stop.** A Decision FALSE arm may be a bare `stop` with no steps. #141 removed the old "reject
path is never a bare Stop" rule: it was legacy *builder* parity, and a real production Decision
routes its false outcome straight to a Stop (capture §2.1). The legacy `flow_sequence` surface still
rejects it — only the direct IR accepts it. A Branch leg / TRUE arm still requires at least one step
before a `stop` terminal, because the empty-leg question is explicitly UNPROVEN (capture §2.4).

**Nesting.** `PROCESS_IR_V1_MAX_CONTROL_DEPTH = 2` counts only `branch`/`decision` on one
root-to-leaf path. This is a **ProcessIR v1 compiler bound chosen on test cost, NOT a Boomi platform
limit** — the platform imposes no observed cap and real production processes exceed it (capture §2.1
records a Decision chain six deep inside one Branch leg). It is enforced three times from two
different representations: on the authored tree at parse, again on the parsed model in
`body_capabilities`, and re-derived from the lowered graph in `check_cfg_invariants`.

**No convergence.** Every leg/outcome terminates independently. There is no join, merge, or
post-control continuation: a second predecessor fails with `PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED`
and a leg reaching no terminal with `PROCESS_IR_SEMANTIC_UNTERMINATED_PATH`. The evidence is
*negative* — no shape in either captured production process (71 shapes) has two inbound edges, and
the KB documents no merge step for integration processes — so the diagnostics say only that
ProcessIR v1 **does not emit** these, never that Boomi would reject them. Examples in this document
must not visually imply convergence.

### 3c. Scoped error handling — the #142 Try/Catch node

`try_catch` protects a region of the flow and routes a failed document to a recovery path. It is
authored as `{scope, try_body, catch_body, retry?}`; both bodies are `steps` + one `terminal`, and
both terminate independently — there is no join and **nothing may follow a `try_catch`**.

**Placement is part of the contract.** `scope` is not a free-form label: each value names a topology
the compiler has a verified emitter shape for.

| Scope | Where it may sit | What its try body must be |
|---|---|---|
| `process` | the **sole root step** | begins with the `connector_call` that produces the flow's documents |
| `connector` | the **last step** of a `connector_call` sequence | optional `set_ddp`/`set_dpp` preparation, then exactly the one `connector_call` it protects |

Both placements are re-checked independently of the parsed model in `body_capabilities`, because
`ProcessIRV1` is exported and mutable — a gate only `parse_process_ir_v1` enforces is not a gate.

**Body matrix.** Both bodies share one step vocabulary; the terminal sets differ.

| Slot | Admitted kinds |
|---|---|
| `try_body.step` | the linear set minus `flow_control`/`data_process`, plus `connector_call` |
| `try_body.terminal` | `stop` |
| `catch_body.step` | same as `try_body.step` |
| `catch_body.terminal` | `stop`, `exception`, `cache_put` (staging sink) |

The asymmetry is deliberate: an `exception` raised inside a protected path would be caught by that
same path's own handler and no evidence covers the loop, while a staging `cache_put` is a recovery
shape rather than a success one. `catch_body` is **mandatory and must terminate** — a caught document
that reaches no terminal is one the process silently drops. For the same reason a catch body whose
terminal is a bare `stop` must contain at least one step: a catch that only stops swallows the
document, that shape is unattested, and the rule reuses the helper #141 applies to a Branch leg for
the same fail-closed-on-unproven reason. A bare `exception` or staging `cache_put` terminal needs no
steps — both do something on their own.

**Retry.** `retry.count` is an integer `0..5`, and that bound is the platform's own: the Try/Catch
step documents exactly that range together with a fixed wait schedule — `0` none, `1` immediate,
then 10s / 30s / 60s / 120s — and auto-retries are skipped entirely in test runs
(`.codex/plans/issue-142-live-captures.md` §G1). **The schedule is platform-owned and not
authorable**; there is no wire field for a delay or a backoff curve, so `retry_backoff_authoring` is
`unsupported` rather than gated. An absent `retry` and an explicit `{"count": 0}` are the same thing
and compile to identical bytes.

**Retry is bounded by SAFETY, not only by range.** Two rules run before anything is emitted:

1. *Source isolation.* A positive count is rejected when the protected region would re-run whatever
   produced the flow's documents (`PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION`). This is derived
   **structurally from the graph** — "is there a producing call strictly upstream of the handler?" —
   not read off the authored `scope`, so a mutated document whose scope disagrees with its shape
   cannot smuggle an unsafe region through. Live evidence: a real process with process-wide retry 2
   reran its entire Get→Map→Send chain on each attempt
   (`docs/archive/2026-06-19-issue-91-capstone-recipe-evidence.md`), and official guidance
   independently advises against a Try/Catch at the start of a process or before a query
   (capture §G7). In practice this means **positive retry is a connector-scope construct**.
2. *Replay safety.* Every retried `connector_call` must sit on a connector-registry row whose
   `retry_safety` permits replay. The registry decides; authored evidence never overrides it.

**Typed idempotency evidence.** `connector_call.idempotency` is a discriminated union, never a
boolean:

- `{"kind": "verified_action"}` — required by an `idempotent_write` row.
- `{"kind": "key_reference", "contract_ref": "$ref:KEY"}` — required by a `conditionally_idempotent`
  row. The reference must resolve in the symbol table **and name the same operation** as the call;
  a contract for a different operation is not evidence about this one.

`contract_ref` accepts an exact `$ref:KEY` token **only** — deliberately stricter than an ordinary
component reference, which also admits a literal id. That is what makes the acceptance criterion
"cannot be satisfied by an unverified free-form Boolean" hold: with a literal-id escape hatch,
`"yes"` would parse as evidence. No key material is ever authored, stored, or emitted.

**Catch always means all errors in V1.** The emitted shape carries exactly two settings — an
all-errors flag and the retry count — and there is **no error-type or error-code list on the wire at
all**, which is why `catch_error_type_lists` is `unsupported` rather than gated: there is nothing to
research later. The flag itself has a documented second value (the platform's default catches only
document-level errors), but the shared renderer fixes the all-errors form together with a matching
label, and changing either would alter bytes for already-shipped processes. That omission is
therefore recorded as `catch_failure_trigger_selection: gated` — a deliberate V1 surface decision
with the semantics fully known (capture §G2/§G3), not an unknown.

**Nesting is gated.** A `try_catch` admits no control node in either body and may not nest. Official
documentation shows that composing two Try/Catch steps silently rewrites the OUTER step's effective
error selection, and that the rule differs depending on whether the two are adjacent (capture §G6) —
so a single deterministic semantic cannot be derived from the authored fields alone.

## 4. Alias normalization (private codec)

The public model has ONE canonical spelling per node. Legacy spellings are normalized only in
the `_process_ir_compat` codec (unexported; since #139A its FORWARD core is REUSED in production
by the flow_sequence legacy adapter, while its reverse codec + strict frozen-scope policy stay
test-only — its compatibility-only machinery, e.g. the `fallback_target` reconstruction of the
legacy-required-but-unemitted root target, carries an M12 removal gate in #147):

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
| `PROCESS_IR_CAPABILITY_UNSUPPORTED` | gated/unsupported construct (keyed cache, `definedparameter`, secret carriage, process-call mixing, a `connector_call` sequence that also authors `source`/`target` or a non-`map_ref` linear step) |
| `PROCESS_IR_SCHEMA_BRANCH_CARDINALITY` | **#141** — a Branch outside the documented 2–25 bound |
| `PROCESS_IR_SEMANTIC_CONTROL_CONTINUATION_UNSUPPORTED` | **#141** — a node authored after a Branch/Decision |
| `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY` | **#141** — a known kind in a body slot the matrix does not admit |
| `PROCESS_IR_SEMANTIC_NESTING_LIMIT` | **#141** — control nesting past the compiler depth bound |
| `PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED` | **#141** — two paths converge on one node |
| `PROCESS_IR_SEMANTIC_UNTERMINATED_PATH` | **#141** — a leg/outcome reaches no terminal |
| `PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID` | **#141** — compiler-derived control wiring is wrong (a compiler defect) |

All seven are **new distinct codes**, not aliases. #140 set that precedent one slice earlier by
registering `PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH` alongside the older
`PROCESS_IR_SCHEMA_INVALID_CARDINALITY` and `PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID` alongside
`PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID`; ADR-001 §7's "later introducers ADD codes" rule is what
permits it. Every pre-existing code keeps every raise site it already had.

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
| `generalized_connector_call` — the `connector_call` node | **supported** | #140 (shipped) |
| `mixed_connector_execution` — multiple connector calls, several families, in one linear path | **supported** | #140 (shipped) |
| `process_call_connector_mixing` — mixing `process_call` steps with connector execution **on one root-to-leaf path** | gated | still gated after #141 (path mode; see §3b) |
| `connector_call_in_control_body` — a `connector_call` inside a Branch leg or Decision arm | **supported** | #141 (shipped) |
| `rich_branch_decision_bodies` — the §3b body matrix, nested Decision, bare false Stop | **supported** | #141 (shipped) |
| `continuation_after_branch_or_decision` | gated | #141 — terminal fan-out only |
| `scoped_try_catch` — the §3c `try_catch` node, both verified scopes | **supported** | #142 (shipped) |
| `bounded_retry` — `retry.count` 0–5 | **supported** | #142 (shipped) |
| `typed_idempotency_evidence` — the §3c evidence union | **supported** | #142 (shipped) |
| `catch_error_type_lists` — catching named error types/codes | **unsupported (permanent)** | #142 — no wire representation exists (capture §G2) |
| `retry_backoff_authoring` — authoring the retry wait schedule | **unsupported (permanent)** | #142 — platform-owned, no wire field (capture §G1) |
| `queue_topology` — creating queues / Event Streams objects | **unsupported** | #142 — out of scope; zero live queue components (capture §G5) |
| `catch_failure_trigger_selection` — choosing document-errors vs all-errors | gated | #142 — semantics known, emitter fixed (capture §G2/§G3) |
| `verified_write_retry_safety` — a stock write action classified replay-safe | gated | #142 — no authoritative classification (capture §G4) |
| `listener_error_scope` | gated | #142 — the fused listener start rejects reliability composition |
| `nested_try_catch` | gated | #142 — composition rewrites the outer error selection (capture §G6) |
| keyed cache (`doc_cache_index`/`cache_key_values`/keyed `load_all_documents`) | gated | no live-captured wire shape (#119) |
| `definedparameter` property source | gated | no verified wire shape |
| joins, loops | gated | ADR-001 §8 |
| caller-authored CFG edges, XML/layout/shape ids, secret values | unsupported (permanent) | ADR-001 §12 |

**`unsupported` means "never", `gated` means "not yet".** #142 uses both deliberately: marking an
impossibility as gated promises follow-up research that cannot conclude, and marking a deliberate
omission as unsupported forecloses a decision that is still open.

`mixed_connector_execution` was **overloaded** before #140: ADR-001 §8 lists it as "multiple
connector calls per path" (which #140 ships), while this document's sequence rules used the same name
for `process_call` × connector mixing (which #140 does not). #140 split the two constructs into two
names rather than pick one meaning and silently redefine the flag.

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

`EmissionPlanV1`'s consumer is the process-emitter registry (#138,
`compiler/process_ir/emitter_registry.py`, `emit_process`) — test-only at #138, and since #139A a
PRODUCTION consumer too (the wrapper + flow_sequence legacy adapters drive it via
`legacy_adapters/emission.py`). It reuses the byte-proven shape
serializers extracted into `process_emitters/` and emits XML byte-identical to the legacy builder for
all 17 pre-#142 emitter kinds (`catcherrors`, key 18, is covered by #142's own anchors — see §3c).
There is still **no MCP/runtime adapter** — the registry is imported directly,
never exported, and invoked by no tool or production builder; #139 owns the production cutover. See
`docs/architecture/PROCESS_EMITTER_REGISTRY_V1.md`.
