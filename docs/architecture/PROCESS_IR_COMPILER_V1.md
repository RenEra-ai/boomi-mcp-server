# ProcessIR Compiler V1 — Internal CFG and Lowering Contracts (issue #137, M12.2)

**Status:** shipped dark. The compiler exists (`src/boomi_mcp/compiler/process_ir/`) and is
covered by parity, determinism, and invariant tests, but **nothing at runtime constructs or
consumes it**: no MCP tool, no builder, no emitter, no JSON Schema, no XML behavior change, and
no deprecation. `flow_sequence` and every other legacy dialect continue through their unchanged
paths until #138 (emitter registry) and #139 (production adapters) reach parity. Pinned by test:
importing `boomi_mcp.models` or `server` must not pull in `boomi_mcp.compiler`.

**References:** [ADR-001](ADR-001-process-ir-authority.md) (§6 authored-vs-derived, §7 error
families, §11 security), [PROCESS_IR_V1](PROCESS_IR_V1.md) (the models this consumes),
[M12 Compatibility Inventory](M12_COMPATIBILITY_INVENTORY.md), issue #137 / epic #134.

## 1. Phases

```
authored payload ──parse──▶ ProcessIRV1 ──lower──▶ SemanticCfgV1 ──lower──▶ EmissionPlanV1
                  (#136)                 (#137)                    (#137)
```

| Phase | Owner | Fails with |
|---|---|---|
| `schema` | #136 `parse_process_ir_v1` | the seven `PROCESS_IR_SCHEMA_*` / `_REFERENCE_*` / `_CAPABILITY_*` codes, translated verbatim |
| `reference_resolution` | symbol-table binding | `PROCESS_IR_COMPILE_EMISSION_PLAN_INVALID`, `PROCESS_IR_CAPABILITY_UNSUPPORTED` |
| `semantic_lowering` | IR → CFG + CFG invariants | `PROCESS_IR_SEMANTIC_*`, `PROCESS_IR_COMPILE_INTERNAL/NONDETERMINISTIC` |
| `emission_planning` | CFG → plan + plan invariants | `PROCESS_IR_COMPILE_*` |

Entry points (`pipeline.py`): `parse_and_compile_process_ir_v1(payload, symbols)` and
`compile_process_ir_v1(ir, symbols)`. Orchestration lives in `pipeline.py` rather than
`diagnostics.py` because `diagnostics` is imported *by* `lowering` and `invariants` — hosting the
entry points there would close an import cycle.

## 2. Why two layers

`SemanticCfgV1` carries control-flow **meaning** only: node identity (an RFC 6901 pointer into the
authored payload), the node's own semantic facts, typed edges, exit roles. It has **no** shape id,
coordinate, dragpoint, or XML state — pinned by test (`test_cfg_carries_no_layout_or_shape_state`).

`EmissionPlanV1` owns everything **generated**: the synthetic Start, the synthetic Stops after
routed targets, `shapeN` identities, geometry, dragpoints, resolved component ids, and emitter
inputs. Synthetic nodes exist only here and carry no authored provenance.

That split is the point of the issue: a caller cannot author reachability, wiring, a shape id, or
a synthetic node, because those concepts do not exist at the layer a caller can reach.

## 3. Contracts

- Every collection is a `tuple` — natively frozen by Pydantic and order-preserving. No `Mapping`
  anywhere: the runtime compiler must not depend on the test-only
  `_process_ir_compat._FrozenMapping`, and re-implementing a frozen mapping would re-open the
  freeze-contract escalation that cost #136 four review rounds. `SymbolTableV1` is therefore a
  sorted tuple with a lookup method, canonicalised on `ref` at construction so caller insertion
  order cannot reach output.
- All models are `extra="forbid", frozen=True`, with `__repr_args__` suppressing every value
  outside a small structural allow-list (mirroring `_ProcessIRBase`).
- Identities are numeric-ascending (`n1..nN`, `e1..eM`, `shape1..shapeN`) and are **never**
  lexically sorted — `shape10` sorts before `shape2` lexically, which would silently reorder a
  plan.
- Edge order is canonicalised at the end of lowering to `(source node ordinal, local ordinal)`.
  A depth-first walk naturally emits a decision's false-arm edge only *after* the entire true-arm
  subtree; canonicalising means the traversal strategy cannot leak into output.
- Canonical JSON reuses #136's recipe exactly: `model_dump(mode="json")` then
  `json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=True)`. `sort_keys` orders
  object keys only, so tuple order survives.

### Provenance grammar

Pointers are **absolute from the document root**, matching #136's `_json_pointer`:

```
/body/steps/{i}
/body/steps/{i}/legs/{j}                      (branch leg edge)
/body/steps/{i}/legs/{j}/steps/{k}
/body/steps/{i}/legs/{j}/terminal
/body/steps/{i}/true_arm|false_arm            (decision outcome edge)
/body/steps/{i}/true_arm|false_arm/steps/{k}
/body/steps/{i}/true_arm|false_arm/terminal
```

Nested control nests further, e.g.
`/body/steps/2/true_arm/terminal/legs/0/terminal` for a target inside a branch inside a decision
true arm.

### Edge kinds

`ordering` · `branch_leg` · `decision_outcome` · `terminal` · `catch`

`terminal` is the sequential edge landing on a node that carries an exit role — it marks the edge
that ends a path, while `ordering` is an ordinary step-to-step edge. `catch` is **reserved for
#142** scoped Try/Catch; V1 generates none and the invariant checker rejects any edge carrying it,
so the reservation cannot rot into an accidental capability.

### Exit roles

| Role | Meaning | Plan consequence |
|---|---|---|
| `stop` | authored `StopNodeV1` | emitted as-is, `origin="ir"` |
| `return_documents` | authored standalone terminal | emitted as-is |
| `exception` | authored `ExceptionNodeV1` | emitted as-is, no Stop follows |
| `routed_target` | a `target` terminating a branch leg or decision true arm | **compiler appends a synthetic Stop** |
| `cache_stage` | a target-less staging leg ending in `cache_put` | **no additional synthetic Stop** — the cache shape itself is the terminal |

## 4. Research ledger (the #137 research gate)

All anchors are in `src/boomi_mcp/categories/components/builders/process_flow_builder.py` unless
noted. Verified by reading the code and by the parity tests that pin these facts against emitted
XML.

**Shape identity and ordering.** Names are purely positional `shape{N}`, 1-based
(`_emit_linear_shapes:4334`); `_emit_stop` even re-derives its index from the name's digits
(`:4221`). `_append_path:5564` allocates indices **depth-first** — linear prefix, then the terminal
block, then each control subtree in authored order — and `_append_decision:5619` /
`_append_branch:5665` append the control shape *before* their subtrees, so index-allocation order
and XML document order coincide. The composed path puts the `[start_noaction, connectoraction_source]`
prefix at shape1/shape2 and starts the sequence at shape3 (`_emit_composed_flow_shapes:5709`).

**Layout.** `_SHAPE_Y=96.0`, `_START_SHAPE_X=96.0`, `_START_SHAPE_Y=94.0`, `_SHAPE_X_STEP=160.0`,
`_DRAGPOINT_X_OFFSET=144.0`, `_DRAGPOINT_Y=104.0`, `_CATCH_SHAPE_Y=456.0`,
`_CATCH_DRAGPOINT_Y=464.0` (`:425-435`); `_shape_x(i)=96.0+(i-1)*160.0` (`:438`),
`_dragpoint_x(i)=_shape_x(i)+144.0` (`:443`). **These are floats and render with a trailing `.0`**
(`x="96.0"`). Carrying them as ints would silently break byte parity for the #138 emitter.

**Dragpoints.** The plain helper `_emit_dragpoints:4231` writes `name`, `toShape`, `x`, `y` and
**no** `identifier`/`text`. Branch (`_emit_branch:4346`) writes the same 1-based integer in both
`identifier` and `text`, all legs sharing one `x` and `y=104.0`. Decision (`_emit_decision:4486`)
writes exactly two, with a **case asymmetry**: `identifier="true"`/`"false"` lowercase but
`text="True"`/`"False"` title-case; the true edge sits on `y=104.0` and the false edge on
`y=464.0` — the only place the catch-row dragpoint Y is reused for a non-catch edge.

**Stop/Return insertion.** There is **no `stop` kind in `_FLOW_SEQUENCE_ALLOWED_KINDS`** — a
legacy caller can never author a Stop, so every `<shape shapetype="stop">` in composed output is
builder-invented: `_target_terminal_entries:5500` (`[target, stop]`), `_append_decision:5648`
(false-arm `[stop]`), `_append_branch:5692` (per-targeted-leg `[target, stop]`),
`_terminal_flow_entry:3072`. **ProcessIRV1 is different**: it has a real `StopNodeV1`, and the
root sequence and decision false arm author one explicitly. The compiler therefore attributes a
Stop to IR whenever IR authored it and synthesises one only after a `routed_target` — the single
case where legacy emits a Stop that IR does not represent. `return_documents.enabled` *replaces*
the Stop (verifier invariant `RETURN_DOCS_STOP_EXCLUSIVE`), and a target-less staging leg gets no
*additional* terminal shape — its last linear shape (the cache write) is emitted with no outbound
wire and is itself the terminal (`:5685-5688`).

**Branch/Decision edge ordering.** Branch legs run in authored order with indices allocated
leg-by-leg (leg *n+1* starts where leg *n* ended). Decision allocates the **true** arm first
(from `decision_index + 1`), then the false arm. The true arm's fallthrough is the top-level
success terminal (`_target_terminal_entries(config)`); the false arm's is its own Stop only.

**Connector-to-map transport shape: none exists.** Three independent lines of evidence — the
complete enumerated `shapetype` literal set contains no transport kind (`grep -ic transport` → 0);
flow assembly is a flat ordered list with nothing inserted unconditionally between the source
connector and the transform slot (`:880-1149`); and `_emit_map:3612` emits a single dragpoint
straight to the next shape name. Pinned by
`test_no_transport_shape_between_connector_and_map`.

**Connector metadata is NORMALIZED, not passed through.** The legacy builder resolves connector
aliases to a canonical subtype and normalizes action case, with role-dependent rules:
`_canonical_connector_type:` maps `rest_client`/`rest` → `officialboomi-X3979C-rest-prod` and
`soap_client` → `wssoapclientsdk`, passing other families through. A **source**
(`_source_prefix_flow_entries:5467`) uses the canonical subtype and upper-cased action for the REST
family, but a **lower-cased** subtype and raw action otherwise; a **target**
(`_target_terminal_entries:5500`, `_branch_target_params:2402`) always uses the canonical subtype
and an upper-cased action. The compiler reproduces these rules exactly, reusing the builder's own
helper so the alias table cannot drift (pinned by
`test_connector_canonicalization_matches_the_legacy_builder`). Passing a symbol's raw alias through
would hand #138 an input that serialises non-parity connector XML — the frozen compat bindings
literally carry `rest_client`.

**Why the compiler canonicalizes rather than trusting the symbol table.** The symbol table is
*specified* to carry canonical connector metadata, so canonicalizing is a **no-op on conforming
input** — `_canonical_connector_type` is idempotent. It is applied anyway because the alternative
to normalizing is not decoupling: detecting non-canonical input requires the *same* alias
knowledge, and the canonical set is open (`database` and any future family pass through verbatim),
so no closed accept-set can be enumerated. Given both options need the knowledge, normalizing is
strictly more useful than rejecting — it turns a contract violation by a future adapter into
correct output instead of silently wrong XML, which is exactly the defect this replaced. The
dependency is a deliberate, drift-tested M12 seam and moves to #139 when the production adapters
own legacy normalization.

**Exception parameter sources are resolved, not passed through.** `ExceptionInputV1` carries a
closed `binding` union — `none` (emits nothing), `current_document` (a bare current
`parametervalue`), or `caught_error` (the fixed `meta.base.catcherrorsmessage` /
`Base - Try/Catch Message` token) — mirroring `_emit_exception_parameters` (builder `:6164`), so
#138 only has to serialise it. This matches how Data Process operations already carry their fixed
wire metadata rather than a raw enum.

**Property names are stripped on the wire.** `_validate_bare_property_name` checks the *stripped*
name but `SetDdpNodeV1`/`SetDppNodeV1` store the original, so `" DDP_X "` is a **valid** ProcessIR
payload. The legacy emitter strips it (`_seq_linear_emit:5443` for the step name;
`_emit_property_source_value:4051`/`:4063` for ddp/dpp source `property_name`; `:4040` for
`profile_type`), so the compiler strips at snapshot time. `default_value` and static `value` are
deliberately **not** stripped, matching the emitter.

**Mapping dictionaries.** `_DATAPROCESS_OPERATIONS:162` (`custom_scripting`→`processtype "12"`
"Custom Scripting", `split_documents`→`"8"` "Split Documents", `combine_documents`→`"9"` "Combine
Documents"); cache aliases `cache_put`→`doccacheload`, `cache_get`→`doccacheretrieve` (with
`loadAllDoc="true"` hard-coded), `doccacheremove`→`doccacheremove` (`_seq_linear_emit:5417-5436`);
DDP/DPP wire prefixes `dynamicdocument.` / `process.` with display names `Dynamic Document
Property - {name}` / `Dynamic Process Property - {name}`, and **DDP `persist` is always `"false"`
on the wire** while DPP honours the authored flag (`_emit_documentproperty_assignment:4077-4102`);
property source `valueType` mapping where `ddp`→`track` and `dpp`→`process`
(`_emit_property_source_value:4016`). `SyncPipelineBuilder._linear_stage_order:6998` derives stage
order from the indegree-0 walk, **not** list position. Verifier terminal sets live at
`process_graph_verifier.py:45-64`.

**Determinism proof.** `test_all_stage_and_dependency_permutations_lower_identically` runs all 12
stage/dependency permutations of a three-stage chain and requires identical lowering *and*
byte-identical XML; `test_input_key_order_cannot_change_output` and
`test_symbol_insertion_order_cannot_change_output` do the same for the compiler itself. The
emission-plan schema was frozen only after these passed.

### Contractual vs incidental

| Aspect | Classification |
|---|---|
| Topology (nodes, edges, reachability, terminals) | **contractual** |
| Ordering (shape index allocation, branch leg order, decision true-then-false) | **contractual** |
| Synthesis (which Stops the compiler owns, the synthetic Start) | **contractual** |
| Wiring (`toShape` targets, dragpoint identifier/text) | **contractual** |
| Geometry (x/y coordinates, dragpoint rows) | semantically **incidental** — the builder's own comment calls it "decorative only; correctness is driven by `toShape` wiring" (`:422`) — but **compatibility-pinned in V1**, because #138 must reproduce current XML byte-for-byte |

### Live observation

A read-only inspection of the `renera` account confirmed the Start→Message→Stop geometry
(`start` at `x="96.0" y="94.0"`, mid-row shapes at `y="96.0"`, dragpoints at `y="104.0"`) and
`<stop continue="true"/>`, matching the constants above. No live component id is recorded here —
the fixtures and this document use sentinel references only.

## 5. Parity oracle

The plan is validated against the **unchanged legacy builder**, not against a hand-written
expectation. `test_process_ir_compiler.py` projects emitted XML into
`(shape name, shapetype, x, y, dragpoints[name, toShape, x, y, identifier, text])` facts and
requires the emission plan to describe exactly those facts for:

- all three golden IR documents (`process_ir_v1.json`),
- all ten frozen codec parity cases (`flow_sequence_compat_cases.json`),
- a constructed decision-with-false-arm-Stop case (no fixture exercises that shape, and it is the
  one process containing both an IR-authored Stop and a compiler-owned Stop).

There is deliberately **no test-only plan→XML emitter**: emission is #138's boundary, and a second
emitter would only prove the two agreed with each other.

## 5a. What the invariant checkers cover

The checkers exist to catch a **compiler** defect, so they are exercised against hand-built
malformed records, not just against real lowering output. Beyond the obvious (duplicate/dangling
ids, unreachable nodes, missing terminals, noncanonical ordinals) they enforce:

- **Exit role agrees with semantics** — a `stop`/`return_documents`/`exception` node must carry its
  role, a `target` may be `routed_target` only, a `cache_put` may be `cache_stage` only, and no
  other kind may claim a role. Without this a Stop with `exit_role=None` reads as a linear node and
  would be planned with an outgoing transition the Stop emitter cannot serialise.
- **Forward-only control flow** — checked *after* reachability, because a fully-reachable, acyclic,
  join-free graph can still be ordered backwards (`n1 -> n3 -> n2`), which would wire a later shape
  to an earlier one.
- **Per-source local ordinals unique and contiguous** — sorted order alone accepts two edges sharing
  `(source, local_ordinal)`, which plan lowering would silently renumber.
- **Transitions match their CFG edges, IN ORDER** — a node's ordered `cfg_edge_id` sequence must
  equal its ordered CFG out-edges, and each transition must target that edge's shape. Per-transition
  checking alone is too weak: swapping *both* the `cfg_edge_id` and `to_shape_id` of a Decision's
  two wires leaves each individually consistent while the position-fixed dragpoint labels route
  `True` down the false arm. The synthetic Start must wire to the CFG entry, and `synthetic`
  provenance is restricted to exactly the Start wire and routed-target Stop wires — otherwise a
  malformed plan could relabel an ordinary wire as synthetic and skip correspondence entirely.
- **`routed_target` is role- and position-checked** — only a `target` endpoint may carry it (a
  *source* marked routed would get a synthetic Stop appended after it), and only where the IR can
  actually author a target terminal: `…/legs/{j}/terminal` or `…/true_arm/terminal`. Keying the
  exit-role table on `semantic_kind` alone missed the role; a bare `/terminal` suffix test missed
  the position, since `DecisionFalseArmV1.terminal` is Stop/Branch/Exception only — a target there
  is unrepresentable, and planning it would append a synthetic Stop on the reject route.
- **Synthetic Stops are inert and declared** — a `terminal_stop` must have no outgoing transitions
  *and* appear in `terminal_shape_ids`. The generic terminal check only inspects shapes that are
  declared, so a multi-exit plan could otherwise wire one synthetic Stop onward to another exit and
  simply omit it from the declaration.

**The plan checker validates the CFG first.** Most plan invariants are stated *against* the CFG and
silently borrow its guarantees: "one plan node per CFG node" borrows id/path uniqueness and canonical
node order; transition-to-edge correspondence borrows endpoint uniqueness, canonical edge order and
edge kinds; routed-target Stop synthesis borrows valid exit roles and positions; and reachability,
acyclicity, join-freedom and forward-only flow are borrowed outright. Because
`check_emission_plan_invariants` is **exported and callable directly**, a caller who skipped
`check_cfg_invariants` would get silent acceptance of a malformed graph rather than a diagnostic — so
it now re-validates the CFG up front. That is O(V+E), cheap against the cost of shipping a plan built
from a broken graph, and it closes the class rather than the instance: this was first noticed as a
single leafless-cycle hole, and enumerating the borrowed invariants showed the hole was general.

**Complexity.** Validation is linear in nodes+edges. Two places matter: CFG out-edges are grouped
by source **once** before the plan-node loop (rescanning `cfg.edges` per node would make it O(V·E)),
and reference resolution runs against an index built **once per pass** via
`SymbolTableV1.build_index()` (the checker resolves every node's references, so a per-reference scan
would make it O(nodes x symbols)). The index is deliberately **not** cached on the model: pydantic v2
includes private attributes in `__eq__`, so a lazy cache makes two identical tables compare unequal
once one is used; `model_copy(update=...)` does not re-run `model_post_init`, so an eager cache goes
stale and silently resolves a present symbol to `None`; and a private attr stays writable despite
`frozen=True`. `SequenceNodeV1.steps` has no upper bound, so neither cost is bounded by the schema.

Both dimensions are guarded structurally (iteration-counting, not wall-clock, so they cannot flake):
`test_plan_validation_never_rescans_cfg_edges_per_node` for the edge dimension and
`test_plan_validation_is_linear_in_symbols_too` for the symbol dimension — the latter exists because
the node-count guards pass an EMPTY symbol table and so are blind to lookup cost. Each was verified
to FAIL when its optimisation is reverted. `test_plan_validation_scales_linearly_with_node_count`
guards this and is calibrated to discriminate (measured: ~8.3× for 8× nodes grouped, ~30× rescanned;
it fails if the rescan returns, and sizes below ~400 do not discriminate at all).
- **Emitter input matches the node's semantics** (and, for connectors, its role), so a Map node
  cannot carry a `MessageInputV1`.
- **Emitter inputs are RECOMPUTED and compared exactly** — the checker re-derives each node's
  emitter input from its CFG semantics plus the symbol table and requires equality. Checking only
  the emitter *kind* plus global component-id membership was far too weak: a wrong semantic value,
  a Stop with `continue_=False`, or a map id belonging to an unrelated symbol all passed.
  Recomputation makes the check total and is simpler than enumerating per-field rules.
- **Control edges are bound to their authored subtree** — a decision outcome must target a node
  under its own `true_arm`/`false_arm`, and a branch leg edge under its own `legs/{j}`. Ordering
  alone would let two targets be swapped while every ordinal stayed valid.
- **`cache_stage` is position-checked** like `routed_target` — it is authored only as
  `BranchLegV1.terminal`, so a root or mid-flow `cache_put` claiming it would mark an ordinary
  linear node terminal and silently truncate the path.
- **Identities and terminal sets are canonical** — `edge_id` must equal `e{ordinal}` (not merely be
  unique), and `terminal_shape_ids` must be exactly the ordered set of shapes with no outgoing
  flow, so duplicates or reordering cannot make two equivalent plans serialise differently.
- **Synthetic Stop adjacency** — a routed target must be *immediately* followed by its own synthetic
  Stop and wired to it; matching counts alone would let the Stop sit anywhere.
- **Branch dragpoint row** — Branch dragpoints all sit on `DRAGPOINT_Y`; unlike Decision there is no
  second row.

## 6. Diagnostics

Every diagnostic carries a stable `code`, the authored RFC 6901 `path`, a `node_identity` (the
nearest authored node path, `<root>` when the pointer names no node), a static `message`, static
`remediation`, and the `phase`. Diagnostics sort by `(phase rank, path, code)` so the earliest
failure in the pipeline reads first.

The `SEMANTIC_*` / `COMPILE_*` split is a contract, not decoration: `SEMANTIC_*` blames the
authored payload, `COMPILE_*` blames the compiler. Reporting a compiler defect as a user error is
how a caller ends up "fixing" correct input.

## 7. Security (ADR-001 §11)

- Messages and remediations are **static strings selected by code**. No authored value, resolved
  component id, or exception text is ever interpolated — including in the raised exception's
  `__str__`, which is what reaches a log. An unexpected exception becomes a bare
  `PROCESS_IR_COMPILE_INTERNAL` with the exception text deliberately discarded.
- `ComponentSymbolV1` accepts only emitter-safe facts: resolved component id/type and derived
  connector/action metadata. Never configuration, credentials, headers, or document content.
- Generated identities are pure ordinals, so they cannot encode a secret.
- `__repr_args__` suppresses every value outside a structural allow-list. Pinned by a sentinel
  test that seeds a marker into every authored and symbol slot and asserts it appears in no
  diagnostic field, no `repr`, and no exception string.

## 8. Boundaries and gates

- **WSS / listener entry is rejected** with #136's `PROCESS_IR_CAPABILITY_UNSUPPORTED` in the
  `reference_resolution` phase. The legacy path *fuses* the start and connector into a single
  `start_listen` shape (`_emit_start_listen:3430`), whereas this compiler always emits the
  `start_noaction` + `connectoraction` pair — so a listener source would be silently mis-shaped.
  Note the guard lives in reference resolution, **not** IR lowering: `ProcessIRV1` has no listener
  node kind at all, so such an entry can only arrive through the symbol table's `connector_type`.
  **#140 settled the alternate entry policy: listener entry stays UNSUPPORTED.** It is now gated in
  two independent places — the pre-existing `LISTENER_CONNECTOR_TYPES` guard for the legacy
  `source`/`target` dialect, and, for a `connector_call`, by simple absence from the closed capability
  allowlist (§10a). No `start_listen` emitter key exists, so there is nothing to cut over to; the WSS
  arm of `sync_pipeline` stays on the legacy renderer until an issue ships that emitter.
- **`return_documents` with a control terminal is unrepresentable** — rejected by both
  `_validate_flow_sequence_config:4733` and the #136 codec, so the compiler has no branch for it.
- The six #137 codes are the **first** codes of the `PROCESS_IR_SEMANTIC_*` and
  `PROCESS_IR_COMPILE_*` families (ADR-001 §7). `PROCESS_IR_CAPABILITY_UNSUPPORTED` is
  **referenced, never re-registered**: `ERROR_TAXONOMY` is a dict comprehension keyed on
  `spec.code`, so a duplicate entry would silently overwrite #136's and flip its owner. Pinned by
  `test_issue_136_codes_still_owned_by_136`.

## 9. Ownership boundaries

**#140** owns the `connector_call` node, its resolution/capability registry, and the mixed linear flow
(§10a) — it adds no emitter key and no MCP surface.
**#137** (this document) owns the internal CFG, the emission plan, and the lowering contracts.
**#138** owns the verified emitter registry that turns an `EmissionPlanV1` into XML — every XML
tag, attribute order, escaping rule, and image name is its boundary, not this one's. **#139** owns
the production legacy adapters. **#141/#142** own the gated control-flow and error-handling
capabilities (continuation after Branch/Decision, scoped Try/Catch — the reserved `catch` edge
kind). **#143** owns CFG-aware semantic validation built on these types.

## 10a. #140 M12.5 — first-class ConnectorCall (shipped dark)

`ConnectorCallNodeV1` (see [PROCESS_IR_V1 §3a](PROCESS_IR_V1.md)) lets one linear sequence contain
many connector calls across several families. It adds **no** emitter key, **no** MCP surface, and no
new pipeline: it lands entirely on the canonical `IR → CFG → plan → emit` chain #139 already made
production.

### Symbol authority — who owns which fact

| Fact | Authority | Why |
|---|---|---|
| operation component id/type, connector family, action | the **operation** symbol | it is what the connector shape emits (`operationId`, `connectorType`, `actionType`) |
| the operation→connection edge | the **operation symbol's `connection_ref`**, populated from the **component plan** | no connector-action component declares its connection (capture FINDING 1), so this cannot be read off the operation component and must not be authored in IR |
| connection component id | the **connection** symbol | |
| request/response profiles of an operation; source/target profiles of a map | the **profile refs on those symbols** | schema metadata, opaque refs |
| entry-vs-downstream placement | **derived from authored position** | a caller cannot author a role (ADR-001 §6) |

`ComponentSymbolV1` therefore gained exactly three optional fields — `connection_ref`,
`input_profile_ref`, `output_profile_ref` — all defaulted, so every symbol every pre-#140 caller
builds is byte-identical.

There is deliberately **no** companion `*_profile_type`: the *profile* symbol's own `component_type`
already is the profile kind (`connector_resolution._profile_identity` reads it from there), and a
second caller-supplied copy would be exactly the duplicate authority ADR-001 §6 exists to remove —
two sources for one fact, with no principled winner when they disagree.

The design plan additionally proposed `dependency_refs` and `binding_variant`; neither shipped, for
the same reason. `binding_variant` (`simple`/`dynamic_path`/`listener`) would have been a second
encoding of a fact the **capability allowlist** already carries — dynamic-path and listener are
rejected by *absence* from it, so a variant field could only ever agree or contradict.
`dependency_refs` was to cross-check that the connection and profile refs appear among the
operation's declared dependencies, but nothing in this tree populates such a field, so it would have
been an always-empty check that reads as verification while verifying nothing. Both remain available
to a later issue that has a real producer for them.

### Where it runs, and in what order

`pipeline.compile_process_ir_v1` gains one stage between the CFG invariant check and plan lowering:

```
lower IR -> CFG  ->  check CFG invariants  ->  validate_connector_calls  ->  lower plan  ->  check plan invariants
```

So every rejection below happens **before an emission plan exists**, hence before any emitter and any
component mutation. A CFG with no `connector_call` node returns immediately — no pre-#140 dialect is
touched.

Within `connector_resolution`, per call, in this order:

1. `operation_ref` resolves to a `connector-action` symbol → else `PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND`.
2. the symbol carries a family and action, and the **canonical** `(family, action)` pair is in the
   capability registry → else `PROCESS_IR_CAPABILITY_CONNECTOR_ACTION_UNSUPPORTED`.
3. an authored `action` agrees case-insensitively with the authoritative one → else the same code, at
   `/…/action`.
4. `operation.connection_ref` resolves → else `PROCESS_IR_REFERENCE_CONNECTION_NOT_FOUND`.
5. that symbol is a `connector-settings` component, it **declares** a connector family, and that
   family agrees canonically with the operation's → else `PROCESS_IR_REFERENCE_CONNECTION_MISMATCH`.
   The family is required, not optional: the emitter does not need it, but this verification does —
   the emitted shape carries the *operation's* family next to *this connection's* id, so an
   unverifiable binding would serialise a REST `connectorType` pointing at a database connection with
   nothing objecting. "Nothing to compare" is not "compares equal". This tightens nothing that
   exists: #139's adapters attach connector metadata only to the *operation* requirement and their
   symbols carry no `connection_ref`, so no pre-#140 symbol reaches this path.

**The capability gate is settled before the connection on purpose.** It is the coarser question: if
the family/action is not supported at all, complaining about the connection would send the reader to
fix something that was never the problem. Within a *supported* family, the connection failure is what
surfaces.

### The capability matrix (`connector_capabilities.py`)

A closed **allowlist**. Anything not a row — an OEM connector subtype, Database V2, WSS `LISTEN`, an
unverified REST verb, a dynamic-path binding — is rejected by **absence**, so no gated-row table is
load-bearing and a family nobody thought to list still fails closed.

| family | action | accepts input | produces documents | side effect |
|---|---|---|---|---|
| `officialboomi-X3979C-rest-prod` | `GET` | none or documents | yes | read |
| `officialboomi-X3979C-rest-prod` | `PATCH` | documents required | yes | write |
| `wssoapclientsdk` | `EXECUTE` | none or documents | yes | read |
| `database` | `Get` | none or documents | yes | read |
| `database` | `Send` | documents required | **no** | write |

`accepts_input` **is** the placement statement, which is why placement is not a third key dimension:
`none or documents` means entry *and* downstream are both supported, `documents required` means
downstream only. Keying on `(family, action, placement)` as well would encode one fact twice.

Both placements are separately evidenced for every `none or documents` row, recorded per row in
`connector_capabilities.py` so the claim is auditable rather than assumed:

| row | entry evidence | downstream evidence |
|---|---|---|
| REST `GET` | `fetch(rest_fetch)` is a verified `sync_pipeline` source stage | official *Get versus Send*: "Documents retrieved by 'Get' connectors used **mid-process** or within another process step (as a look-up, for example)" |
| SOAP `EXECUTE` | `fetch(soap_fetch)` source stage (byte golden `sync_pipeline_soap_fetch_soap_send.xml`) | `send(soap_send)` target stage of the same shipped chain |
| Database `Get` | `read(db_read)` source stage | live capture FINDING 5 — a real process runs a database `Get` mid-flow off a `catcherrors` leg |

Lookup case-folds the **action** only; the family is an opaque account-scoped string, and the
**emitted** action spelling always stays the authoritative one (`Get`/`Send` stay mixed-case for the
database family — `SEND` would be a different wire value).

### Flow semantics

- **Cardinality** (`PROCESS_IR_SEMANTIC_CARDINALITY_MISMATCH`): a `documents required` call may not be
  the entry call, must follow a producing call, and **nothing that consumes documents may follow a
  call that produces none**. That last rule reaches the **terminal** too, and the two terminals are
  not interchangeable: `stop` consumes nothing and merely ends the path (the legacy `[target, stop]`
  shape), while `return_documents` returns the current document stream to the caller — so
  `… → Database Send → return_documents` is rejected, because it would emit a Return Documents shape
  that can never return anything.
- **Profile well-formedness** (`PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`): on **every** call, a
  *declared* profile reference must resolve to a real profile component — one of the five `profile.*`
  types — not only on the two calls that happen to sit beside a map. This is distinct from profile
  *equality* below: a ref naming a connection or a map is not a weaker match, it is not a profile at
  all, and accepting it would let the compiler claim it "verified profiles" having verified nothing.
  An **absent** ref is deliberately not an error here — official documentation describes connector
  profiles as optional ("Request Profile … when provided") and this checkout has no per-family
  required-vs-optional evidence; where a profile *is* required (both sides of a map) absence is
  already a mismatch below.
- **Profile continuity** (`PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`): around every `map_ref`, the map's
  source profile must be the preceding call's output profile and its target profile the following
  call's input profile. Compared by **resolved component id** plus normalized profile type, so two
  refs naming one component (#139B's occurrence-scoped aliases) agree. Two guards keep the comparison
  from being vacuous: an **absent** profile on either side is a mismatch (a map's profiles are hard
  component requirements, and "not declared" cannot satisfy one), and a ref must resolve to an actual
  profile component — one of the five `profile.*` types — or it has no profile identity at all.
  Without the type check the test would be self-fulfilling: two refs both naming the same *connection*
  would compare equal and the map would "match" with neither side a profile.

### Two evidence-forced decisions

**`Send` is terminal.** Official Boomi documentation states a `Send` action "sends data to a given
destination but **does not return any data to the process for further processing**", and Database
(Legacy) declares no response profile at all — only a Write profile, which the profile documentation
describes as the *destination* profile in a map. The checkout agrees: every legacy emission path ends
a connector target as `[target, stop]`, and the one live `database/Send` step captured goes straight
to a `stop`. So the issue's literal step order (`… → DB Send → REST PATCH → terminal`) is **rejected**
with a stable cardinality code rather than emitted as a shape that could never run, and the shipped
representative flow places the Send last:

```
REST GET → MapRef → SOAP EXECUTE → REST PATCH → Database Send → Stop
```

**Profile continuity is enforced around a map, not between adjacent calls.** The official REST Client
operation page says of a Response Profile: "Selecting a profile does not validate or guarantee that
output will follow the provided format." The platform does not enforce call-to-call profile equality,
so the compiler does not invent it — it equality-gates only the map boundaries, which are real
component requirements. It tracks whether the preceding call **produced documents at all** (for the
cardinality rule); it deliberately does **not** carry a running "effective profile", because doing so
would mean asserting, at each step, an identity the platform does not guarantee. Consistency check: the
issue's own flow goes `SOAP EXECUTE → DB Send` with no intervening map, so a strict call-to-call rule
would have made the issue's own representative flow unbuildable for a second, unrelated reason.

### Why there is no binding table in the emission plan

`check_emission_plan_invariants` **recomputes** every emitter input through
`lowering._emitter_input_for(node, symbol_index)` and compares exactly — that recomputation is what
makes the plan check total. A connector call's emitter input is fully derivable from the CFG node plus
the symbol index (the entry/downstream role rides on the CFG semantic, exactly as
`ConnectorSemanticV1.role` already does), so no signature changed and nothing downstream carries a
second copy of the same facts. Carrying one would give the checker something to compare against itself
instead of against the symbol table — strictly weaker. `PROCESS_IR_COMPILE_CONNECTOR_BINDING_INVALID`
is consequently reachable only by bypassing the pipeline's resolution phase, which is why it is a
`COMPILE_*` (compiler-defect) code and not a caller-facing one.

### Goldens and their oracle

The legacy builder cannot express a multi-connector flow, so there is **no legacy parity oracle** for
`tests/fixtures/process_ir/emitter_parity/connector_call_mixed.process.xml`. Four independent checks
stand in for one, each asserted in `tests/test_connector_call_mixed_flow.py`: the `connectoraction`
attribute set is identical to the shipped, live-QA-verified `sync_pipeline_*` goldens (same unmodified
renderer); `verify_process_graph` accepts the emitted XML; the emission-plan invariants hold; and the
bytes are stable across repeated compiles and shuffled symbol order.

## 10. #138 M12.3 — the test-only emitter registry (shipped)

The verified process-emitter registry named as #138's boundary in §9 now exists at
`src/boomi_mcp/compiler/process_ir/emitter_registry.py`. It is the first consumer of
`EmissionPlanV1`: `emit_process(plan, symbols) -> ProcessEmissionArtifactV1` dispatches each planned
shape to a typed emitter keyed by the closed `emitter_kind` discriminator, reusing the byte-proven
serializers now extracted into `categories/components/builders/process_emitters/`. It stays **DARK /
test-only** (imported directly, never through this package's `__all__`; no MCP tool or production
builder invokes it) until #139's production cutover. Contract, the 17-key manifest, the fail-closed
diagnostic mapping (the five new `PROCESS_IR_COMPILE_*` codes + the reused
`PROCESS_IR_COMPILE_INTERNAL`, plus the `xml_emission`/`post_emission_verification` phases), and the
byte-parity evidence: **`docs/architecture/PROCESS_EMITTER_REGISTRY_V1.md`**.

## 11. #141 M12.6 — rich Branch/Decision bodies (shipped dark)

Branch legs and Decision arms may now hold `connector_call`, `process_call` and nested `decision`
nodes (the exact matrix and its live evidence: [PROCESS_IR_V1 §3b](PROCESS_IR_V1.md)). Three compiler
changes carry it; the emitter registry is untouched.

### Per-path document dataflow (replaces the flat spine)

`connector_resolution.validate_connector_call_semantics` used to walk ONE flattened list of every
`connector_call` and `map` in CFG order. That is correct only while the flow is linear — with calls
inside Branch legs the list interleaves independent paths, so leg 2's first call would be judged
against leg 1's last one. It is now a depth-first walk of the CFG (a tree by this point, since
`check_cfg_invariants` has already rejected joins and cycles) carrying a `_PathState` that is
**copied, never shared, across every control edge**. That copy is the sibling isolation.

State carried per path: whether anything upstream yields documents, the upstream *binding*
specifically (kept apart, because a legacy `source` endpoint produces documents but carries no
profile refs — treating it as a map's upstream would compare against nothing), a non-producing call
that nothing may follow, and a map awaiting its downstream call.

Two ordering rules are load-bearing and were both found by test:

* the **Send gate is checked before** the `documents_required` rule, because a non-producing
  predecessor also leaves the producer slot empty — checking the other order blames the follower for
  a defect that belongs to the Send;
* the gate **blames the Send**, not its follower. #140 shipped that pointer and it is the actionable
  one: the Send is the node whose position is wrong (it must be last on its path), whereas the
  follower may be a perfectly good call that simply cannot be reached.

A map's profiles are compared only when it sits between two connector **calls**. A map whose upstream
is a legacy endpoint or a cache read is left unchecked exactly as before #141 — claiming to have
"verified profiles" there would be a claim about something never compared.

### CFG invariants

* The ConnectorCall entry-role rule is restated: exactly the node that IS the control-flow entry may
  carry `role="entry"`, and nothing else. The old "if any call exists, exactly one is the entry" form
  was correct only while calls lived on the root spine; under a control-only root the entry is the
  control node and no call is the entry at all.
* Every Branch leg / Decision outcome must independently reach an exit
  (`PROCESS_IR_SEMANTIC_UNTERMINATED_PATH`). This is strictly stronger than the existing "no path
  ends on a non-terminal" rule, which only inspects leaves: one leg can route into a subtree whose
  leaves are all valid exits while a *sibling* reaches none.
* A second predecessor is `PROCESS_IR_SEMANTIC_JOIN_UNSUPPORTED`.
* Control depth is re-derived from the lowered graph, deliberately a different representation from
  the authored-tree checks, so a lowering defect that flattened nesting cannot slip past a rule that
  only ever looked at the authored form.
* Branch/Decision wiring defects (count, order, labels, target row) now raise
  `PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID`.

### What did NOT change

`contracts.py`, `connector_capabilities.py`, `emitter_registry.py`, both XML emitters,
`process_flow_builder.py`, `process_graph_verifier.py`, `_process_ir_compat.py` and both legacy
adapters are untouched. The CFG contracts were already sufficient — `branch_leg`/`decision_outcome`
edges, `leg_ordinal`, typed outcome and the existing exit roles carry rich bodies unchanged — and a
control-only root needed **no** lowering or plan change at all (verified by probe before any edit:
the synthetic Start wires straight to a control entry, legs number correctly, and the plan invariants
pass).

### Round-1 review corrections (#141)

Four holes the first cut of the per-path walk left open, all specific to rich bodies — before
#141 none of these shapes were expressible, which is why the pre-existing rules were complete
for what they covered and incomplete for what #141 added:

* **The Send gate now covers every downstream node**, not just a following call or map. A root
  `connector_call` sequence admits only calls and maps, so those two kinds *were* the whole story;
  a Branch leg may put a `message`, a routed `target`, a nested control or a property step after a
  Send, none of which can execute. Only a plain `stop` (consumes nothing) or a stream-replacing
  cache read (supplies its own documents) may follow. The one asymmetry preserved from #140: a
  `return_documents` terminal is still blamed on the terminal, every other follower on the Send.
* **Every map is validated or rejected.** A second map used to overwrite the first `pending_map`,
  and a map reaching a path exit was dropped — so the compiler could claim to have verified
  profiles for a map it never compared. A pending map must now be answered by the very next node
  being a `connector_call`. This restates in the walk what #140 states in the MODEL for root
  sequences, because inside a body a map is an ordinary linear step the model cannot constrain.
  A map whose upstream is a legacy endpoint still carries no call-to-call pair and stays unchecked.
* **ProcessCall bodies require a control-only root** — see PROCESS_IR_V1 §3b.
* **`_check_every_control_path_terminates` runs BEFORE the generic successor/leaf rules.** In a
  finite acyclic join-free CFG a leg that reaches no exit always ends on a non-terminal leaf, so
  the generic check fired first and `PROCESS_IR_SEMANTIC_UNTERMINATED_PATH` was unreachable. A code
  with no reachable path is not a check; it shipped that way because the first cut added the code
  without a test that exercised it.

### Architect impl-review corrections (#141)

* **A map must follow its call IMMEDIATELY.** Carrying the last producing call across
  intervening nodes let `call → message → map → call` read as bracketed, comparing the map's source
  against a call that no longer feeds it (a Message *replaces* the document). Branch/Decision stay
  transparent — they route documents without altering them — so `call → branch → [map, call]` is
  still checked. A map with a call upstream but not immediately before it is now rejected; a map in
  a **pure legacy** flow (no call anywhere on the path) stays unchecked exactly as before #141.
* **Whole-region provenance.** The per-node rule bound only a control edge's FIRST target to its
  leg/arm; the whole reachable region is now checked, so a subtree escaping into a sibling region
  one node later is caught too.
* **Control-wiring precedence.** A control node's transition/edge mismatch raises
  `PROCESS_IR_COMPILE_CONTROL_WIRING_INVALID` rather than losing the race to the generic
  plan-invalid code, which previously left the specific code reporting only label/geometry defects.
* **Every #141 code has its own remediation** in the compiler diagnostic table; a generic fallback
  is not a contract.
* **Whole-document rules name the offending node.** Depth and ProcessCall-mixing moved out of
  `ProcessIRV1` model validators (which can only ever attach to the document root) into a
  post-parse walk carrying the authored path.
* **A known kind in a disallowed body slot** reports
  `PROCESS_IR_CAPABILITY_NODE_NOT_ALLOWED_IN_BODY`, not `PROCESS_IR_SCHEMA_UNKNOWN_NODE` — calling a
  documented kind "unknown" sends the caller to fix the wrong thing. A genuinely unknown tag keeps
  the unknown-node code.
* **A Decision arm admits at most ONE `process_call`**; the capture attests exactly one
  `decision →true→ processcall`, and a chain is unproven. The Branch-leg rule stays plural, as the
  reconciliation states it.

## 12. #142 M12.7 — scoped error handling and retry safety (shipped dark)

Adds `try_catch` to the compiler. The surface stays dark until #146; nothing in `build_integration`
gained a ProcessIR field.

### 12.1 Error regions are DERIVED, never stored

`compiler/process_ir/error_handling.py` is the single authority. `derive_error_regions(cfg)` walks
each `try_catch` node's two subtrees and returns them as node-id sets; `catch_region_node_ids(cfg)`
is the union of the recovery ones.

**Nothing carries a region id or an "is on the catch row" flag.** Lowering calls
`catch_region_node_ids` to place shapes, and `check_emission_plan_invariants` calls it *again* to
verify them. That is the point: a stored membership flag would make the checker compare lowering's
answer against lowering's own note rather than against the graph — the duplicate-authority failure
#140 removed for connection refs and #141 for control-branch membership.

The walk runs **last** in `check_cfg_invariants`, after reachability, join-freedom, cycle and
forward-only checks have proven the graph is a tree. Running it earlier would force it to defend
against a cycle the checker directly above it already rejects.

### 12.2 Source isolation is a graph property, not an authored one

`PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION` fires when a positive retry's region would re-run
whatever produced the documents. The question asked is **"is there a producing node strictly
upstream of this handler?"** — walked over the CFG's unique predecessor chain, never read off the
authored `scope`.

That matters: `scope` is a string on a mutable model. A document whose scope says `connector` while
its shape puts the handler at the root would pass a scope-based check and still re-run its source.
Deriving the answer from the graph makes the two impossible to disagree.

Consequence: **positive retry is in practice a connector-scope construct.** Every supported
process-scope body begins with the producing call, so a positive count there is always rejected.

### 12.3 Retry safety comes from the registry, never from the payload

`ConnectorCapabilityV1` gained a **required, un-defaulted** `retry_safety` column
(`read_only` | `idempotent_write` | `conditionally_idempotent` | `non_idempotent` | `unverified`).
No default is the fail-closed mechanism: a new row cannot forget to classify itself, because
omitting the field is an import-time error rather than a silent "safe".

It is a **separate fact from `side_effect`** and must not be inferred from it. `side_effect` answers
"does this change anything?"; `retry_safety` answers "may this run twice?". SOAP `EXECUTE` is the
row that proves the distinction: it is `side_effect="read"` and `retry_safety="unverified"`, because
one generic action covers every operation a service exposes.

| Row | `retry_safety` | Obligation when retried |
|---|---|---|
| `read_only` | replaying re-reads | none |
| `idempotent_write` | replay is safe | `{"kind": "verified_action"}` |
| `conditionally_idempotent` | safe under a contract | `{"kind": "key_reference"}` resolving to the SAME operation |
| `non_idempotent`, `unverified` | never retryable | — rejected regardless of evidence |

**No production row ships as `idempotent_write` or `conditionally_idempotent`.** REST `PATCH`, SOAP
`EXECUTE` and Database `Send` are all `unverified`: the knowledge base returns no authoritative
retry-safety answer for any of them, and the one on-point official statement makes the *caller*
responsible for ensuring retries are safe rather than promising the connector is
(`.codex/plans/issue-142-live-captures.md` §G4). Those two branches are exercised by **synthetic
rows in tests only** — which is honest about coverage rather than shipping an unbacked claim.

Evidence **discharges an obligation; it never grants permission.** The `_NEVER_RETRYABLE` check runs
*before* the evidence check precisely so an authored assertion cannot promote an unclassified write.

### 12.4 The catch path forks from scope-entry state

In `connector_resolution._walk_paths`, a child pushed on a `catch` edge gets a state that:

- **drops** the protected path's progress (`pending_map`, `map_upstream`, `producer_binding`,
  `blocked_by`). A Set Properties inside the try body has not necessarily run — that is the failure
  being caught — so the recovery path must not be validated as though it had;
- **marks a document present**, because the platform hands the failed document to the recovery path
  even when nothing upstream produced one (live evidence: a retried process routed one error
  document to its catch leg);
- **keeps `saw_call` true**, so an unbracketed map in a catch body fails closed through the existing
  "a call ran upstream but is not this map's immediate predecessor" rule rather than being silently
  skipped the way a pure-legacy map is.

`try_catch` joins `_MAP_PAIRING_TRANSPARENT`: like Branch/Decision it routes documents without
altering them, so `call -> try_catch -> [map, call]` stays profile-checkable.

### 12.5 Validation order (HISTORICAL — superseded by #143 §13)

> **Superseded.** This describes the #142 connector-scoped order and remains accurate for
> `validate_connector_calls` itself, which is unchanged. Since #143 (M12.8) the NORMATIVE
> validation order for the canonical path is the unified semantic pipeline — see §13 below and
> [PROCESS_IR_SEMANTIC_VALIDATION_V1](./PROCESS_IR_SEMANTIC_VALIDATION_V1.md).

`validate_connector_calls` remains the single entry point and now runs three passes:

```
resolve_connector_call_bindings   ->  validate_error_handling  ->  validate_connector_call_semantics
```

Retry safety is checked on the **bindings resolved once above**, not on a second resolution — the
gate must see exactly what the emitter will see. It runs **before** ordinary flow semantics so an
unsafe retry reports as an unsafe retry rather than as whichever cardinality complaint the same
payload happens to trip. Keeping it inside the one entry point avoids a second public path that
enforces the connector gate but not the retry gate.

### 12.6 Lowering and geometry

- Two edges per handler: Try = `ordering` local 1, Catch = `catch` local 2. The `catch` edge kind's
  #137 reservation is **lifted**; the invariant checker now rejects it out of any node that is not a
  `try_catch` (`PROCESS_IR_COMPILE_ERROR_REGION_INVALID`) instead of rejecting it everywhere.
- The whole Try subtree is allocated before the Catch subtree, which keeps the graph forward-only
  and reproduces the shipped Try/Catch shape ordinals.
- Geometry, transcribed from the shared renderer's own constants: catch-row shapes at
  `CATCH_SHAPE_Y = 456.0`, their transitions at `CATCH_DRAGPOINT_Y = 464.0`; the handler's own
  dragpoints are `default`/"Try" on the normal row and `error`/"Catch" one row down.
  `CATCH_DRAGPOINT_Y` and `DECISION_FALSE_DRAGPOINT_Y` hold the same number and stay two names —
  two independent facts that happen to coincide.
- `CatchErrorsInputV1` carries **only** `retry_count`. `scope` is absent because graph placement is
  the authority; `userlabel` is absent because the renderer composes its own, so a field carrying
  one would never reach the wire.

### 12.7 Failure taxonomy

| Code | Fires when |
|---|---|
| `PROCESS_IR_SCHEMA_RETRY_COUNT` | retry count is not an integer 0–5 |
| `PROCESS_IR_CAPABILITY_ERROR_SCOPE_UNSUPPORTED` | unknown scope, or a known scope in an unverified placement |
| `PROCESS_IR_SEMANTIC_RETRY_SOURCE_REEXECUTION` | the retried region contains the flow's producer |
| `PROCESS_IR_SEMANTIC_RETRY_NON_IDEMPOTENT_WRITE` | a retried call's row is `non_idempotent`/`unverified` |
| `PROCESS_IR_SEMANTIC_IDEMPOTENCY_EVIDENCE_MISSING` | evidence absent, wrong kind, unresolved, or bound to another operation |
| `PROCESS_IR_SEMANTIC_CATCH_UNTERMINATED` | the catch body reaches no terminal |
| `PROCESS_IR_COMPILE_ERROR_REGION_INVALID` | a derived region is structurally impossible — a compiler defect |

Every code has at least one test that actually reaches it, and every rejection is paired with a
positive case that compiles: a code tested only negatively can still be vacuous.

**One defect class, one code, at every depth.** `_check_region_containment` takes its code as a
*parameter* rather than hard-coding one. Branch/Decision callers keep
`PROCESS_IR_SEMANTIC_AMBIGUOUS_FLOW` (the default, so #141's shipped diagnostics do not move); the
Try/Catch caller passes `PROCESS_IR_COMPILE_ERROR_REGION_INVALID`. Without that, a node whose
provenance escaped its own error body reported a compiler-blaming code when it was the edge's first
target and a caller-blaming one when it sat a node deeper — the diagnostic would have depended on how
deep the corruption happened to be rather than on what kind of defect it was.

**Diagnostic precision at the parse layer.** Two remappings are deliberately narrow:

- the gated-extra-key remapping matches the **immediate owner** (the discriminator tag directly
  preceding the offending key), not the whole error location. A membership test would also fire for
  a node *nested inside* a `try_catch` — a catch-body Message with a stray `backoff` would be sent to
  the capability manifest to read about a gated feature when it simply has an unknown field;
- the body-slot remapping **excludes** locations beneath `idempotency`, which is a tagged union but
  not a body slot. `{"kind": "message"}` there hits a tag that is a real node kind inside a body
  location, and would otherwise report "message is not admitted in this control-body slot" — wrong,
  and self-contradictory, since Message *is* admitted there.


## 13. #143 M12.8 — unified semantic validation (shipped)

Full contract: [PROCESS_IR_SEMANTIC_VALIDATION_V1](./PROCESS_IR_SEMANTIC_VALIDATION_V1.md).
Migration matrix: [M12 Compatibility Inventory](./M12_COMPATIBILITY_INVENTORY.md) §7.

### 13.1 Where the gate runs — and where it deliberately does NOT

`compile_process_ir_v1` is **unchanged**. The plan placed the gate inside it; that was implemented
and measured, and it breaks the legacy parity suites for TWO independent reasons.
First, structurally: canonical validation is deliberately stricter than the legacy surface, the
legacy surface keeps its behaviour through exemptions keyed on ADAPTER IDENTITY, and
`compile_process_ir_v1(ir, symbols)` does not know which adapter produced its IR. Second, and
covered by no exemption: the compiler's own fixtures use placeholder component types, which the
reference phase reports as `…REFERENCE_COMPONENT_TYPE_MISMATCH`. A faithful reproduction measures
**20 failing tests** — 17 / 7 / 3 across those three codes (7 tests carry two). An earlier version
said "exactly" the two exemption-covered codes; that was wrong.

The gate therefore runs at the two places that know their context:

| Site | Gates | Policy |
|---|---|---|
| `integration_builder._process_component_preflight` | plan/apply for authoring actions | strict |
| `legacy_adapters.emission.emit_legacy_result` | the canonical `compile → emit` chain | the named dialect's exemptions |

`emit_legacy_result(result, *, resolver=None, dialect=None)` — with `dialect=None` the gate is
SKIPPED, not run strictly. Running strictly with exemptions unavailable is the same mismatch that
makes a compiler-internal gate unshippable.

### 13.2 Normative order on the canonical path

```
parse → body capabilities → lower to CFG → CFG invariants → connector calls
      → lower to emission plan → plan invariants → [#143 semantic report] → emit → verify
```

The report validates the CFG that was **just lowered**, not a fresh one — the gate must judge exactly
the graph about to be emitted.

### 13.3 What did NOT change

- `validate_connector_calls` and its #140/#142 codes — `port-unchanged`, and the flow collector
  DELEGATES to it rather than re-deriving it, so map-bracketing and the non-producing-connector rule
  keep one implementation.
- `check_cfg_invariants` / `check_emission_plan_invariants` — still compiler oracles, still raising.
- The post-emission graph verifier — still separate, still post-emission, still classified as a
  compiler/emitter defect on failure.
- `cache_property_lineage` — `adapter-only-compat`. The new lineage module does not import it; the
  wildcard default stays on the legacy surface only.
- All 40 raw-byte XML goldens — byte-identical.
