# MCP authoring workflow V1 — discovery, plan, compile, apply, verify

**Status:** landed. **Owner:** issue #146 (M12.11), epic #134.
**Depends on:** #143 (unified semantic validation), #144 (`SystemTopologySpecV1`), #145 (typed
recipe contributions), #135 (ADR-001 authority/capability policy).
**References:** [ADR-001 §5, §6, §7, §9, §11](ADR-001-process-ir-authority.md),
[SYSTEM_TOPOLOGY_V1](SYSTEM_TOPOLOGY_V1.md),
[TYPED_RECIPE_CONTRIBUTIONS_V1](TYPED_RECIPE_CONTRIBUTIONS_V1.md).

---

## 1. The problem this closes

A canonical compiler is not enough on its own. The MCP layer can still mislead if discovery,
schemas, advisory planning, compilation, mutation and verification describe different contracts.

The originating evidence was concrete: **the live MCP service reported four archetypes while the
checkout registry had six.** Nothing was broken — the deployment was simply older — but a client
had no way to *find that out*. A capability catalog you cannot compare is a capability catalog you
cannot trust.

So #146 does two things:

1. One **contract registry** (`boomi_mcp.authoring.contract`) feeds discovery, schema retrieval, the
   `build_integration` dispatcher, the workflow schema, and the wrapper docstrings. They cannot
   advertise different actions or selectors, because there is only one set to advertise.
2. Every phase publishes a **revision binding**, and a typed apply must reproduce it before its
   first write.

No sixth tool is introduced. `compile` is an additive action on `build_integration`.

## 2. The eight-step sequence

| # | Call | Mutates Boomi |
|---|---|---|
| 1 | `list_capabilities()` | no |
| 2 | `get_schema_template(schema_name="AuthoringRequestV1")` | no |
| 3 | `plan_integration_design(...)` | no |
| 4 | `build_from_archetype(...)`, or author ProcessIR / recipes | no |
| 5 | `build_integration(action="plan", config={"authoring_request": …})` | **no** |
| 6 | `build_integration(action="compile", config={"authoring_request": …})` | **no** |
| 7 | `build_integration(action="apply", …)` | **yes** — the first phase that may |
| 8 | `build_integration(action="verify", config={"build_id": …})` | no |

### Authoring ProcessIR directly (#146 amendment)

A caller who intends to author `ProcessIRV1` directly does not need an archetype, and the sequence
above no longer pretends otherwise. `plan_integration_design(authoring_mode="process_ir")` returns
`mode="process_ir_pre_selection"`: **no archetype is reported as a missing input**, and discovery
starts at the revision surface rather than the archetype catalog.

| # | Call | What it gives you |
|---|---|---|
| 1 | `list_capabilities()` | served selectors, capability states, the three revisions |
| 2 | `get_schema_template(schema_name="ProcessIRV1")` | the **grammar** — what a document may contain |
| 3 | `get_schema_template(schema_name="process_ir_authoring", …)` | the **behaviour** — what the constructs mean |
| 4 | `plan_integration_design(authoring_mode="process_ir")` | doctrine, capability gaps, the apply refusal |
| 5 | `build_integration(action="plan", …)` | semantic validation against your document |
| 6 | `build_integration(action="compile", …)` | canonical compile + the compile hash |

There is deliberately **no step 7**. A direct ProcessIR intent is plan/compile-only while
`authoring.typed_apply.process_materialization` is published `unsupported` (§11, Decision 1), and the
brief says so in its own gap list rather than leaving the reader to notice the sequence stopped early.

`authoring_mode` must be requested explicitly. `intent_flags` never selects it — a relevance hint
that could switch a response shape is a hint that changes an answer nobody asked to change.

### The behavioural authoring contract

`get_schema_template(schema_name="process_ir_authoring")` serves one addressable entry per public
ProcessIR node, capability, body placement, connector action, diagnostic, state scope, doctrine
pattern and recipe. It exists because the JSON Schema states the **grammar** and nothing states the
**semantics**: before it, a caller could not learn from any MCP response that Branch paths run in the
authored order, which node kinds a Decision's false arm admits, or that retry over an action with no
established replay safety is refused whatever evidence they attach.

**Retrieval is budgeted.** A bare call returns the schema, the facets and **zero** entries — ask for
what you need. Filters (`authoring_entry_id`, `node_kind`, `category`, `capability_id`,
`workflow_stage`, `after_entry_id`, `limit`) AND together, results sort by `contract_entry_id`, and a
64 KiB payload budget applies on top of the 1–50 count bound. An unknown *enumerated* value is
`INVALID_INPUT` naming the facet; an unknown *exact* entry id is a **success with zero entries**, so a
stale citation is observable rather than indistinguishable from a malformed request.

**One capability vocabulary.** Everything is published as `supported` / `gated` / `unsupported`, with
each entry also carrying its authority's own `source_state` verbatim and the published mapping from
the three pre-existing vocabularies. `gated` ("not yet") and `unsupported` ("never") never collapse.
Doctrine's `guidance_only` and `na` map to `unsupported` with `applicable: false` — they are advice,
not a capability that was withdrawn.

**Every entry names its source** and whether it was `generated` from a named runtime registry or
`parity_pinned` against one. See ADR-001 §6 for why the projection is admitted at all and what it may
never carry.

### Diagnostics carry their own repair

A blocked plan or compile no longer genericizes the validator. Each diagnostic carries the canonical
code in `cause_codes`, the authored path and `node_identity`, the validator's **own** static
remediation, re-validated structural `evidence`, and `authoring_contract_entry_ids` — every one of
which resolves through `get_schema_template(schema_name="process_ir_authoring",
authoring_entry_id=…)`. **No served remediation cites a repository artifact**, because no MCP tool
can fetch one.

A typed request whose `process_ir` document is malformed is reported by **ProcessIR's own parser**:
stable `PROCESS_IR_*` code and an RFC 6901 pointer into the authored payload
(`/intent/process_ir/...`), not a pydantic `loc`/`type` pair. Pydantic's `input`, `ctx` and `msg` are
never serialized, so the authored value never travels.

### Three planning concepts, deliberately distinct

* `plan_integration_design` — **advisory**. Doctrine, gaps, and typed next steps. It never turns
  prose into executable intent, and it never builds an `AuthoringRequestV1` for you.
* `build_integration(action="plan")` — **semantic**. Validation, resolved references, capability
  gaps, required decisions, and the `IntegrationSpecV1` ComponentPlan preview.
* `build_integration(action="compile")` — **canonical**. Normalized intent, deterministic artifact
  fingerprints, and the compile hash apply binds to. Returns **no `build_id`** — nothing was created,
  so there is nothing to identify.

### The typed apply envelope

A typed apply — once the profile resolves — always answers in the typed envelope: `action`,
`mutation_performed`,
`mutation_status`, and — when it REFUSED before writing anything — an `error_code`. A *successful*
apply never carries one, including an all-`reuse` apply that legitimately wrote nothing.

`mutation_status` is the honest one, because a single boolean was answering two questions that
diverge exactly where it matters — *must the caller reconcile?* and *is this failure retry-safe?*

| `mutation_status` | meaning | `mutation_performed` | retry-safe |
|---|---|---|---|
| `performed` | a writing step SUCCEEDED and returned a component id — the write was observed | `true` | no |
| `possible` | a writing step was attempted and this server cannot confirm the outcome — it failed, or succeeded without returning an id | `true` | no |
| `none` | only `reused` bindings, or nothing attempted at all | `false` | **yes** — a FAILED apply in this state carries an error code (see below); a successful one carries no error at all |

The code on a failed `none` apply is whichever one the refusal already named — `AUTHORING_PLAN_STALE`
for a stale binding, `AUTHORING_CAPABILITY_REVISION_MISMATCH` for a moved contract, `INVALID_INPUT`
for a schema-invalid request — and `AUTHORING_APPLY_VALIDATION_REQUIRED` only when nothing more
specific applied. **Branch on `mutation_status`, not on one code**: all `none` refusals are equally
retry-safe, and a client keying on a single token would miss three of them.

An id alone is not enough for `performed`: a failed update carries the TARGET id it was aiming at
(a pre-write component GET that times out returns one), so the step's own result must have succeeded.

`possible` does not distinguish "the request committed and the response was lost" from "the request
was rejected" or "no request was issued".

That is a deliberate under-classification, and the honest reason is narrower than "there is no
signal". Some failures DO carry structured evidence — a pre-write component-GET timeout returns
`error_code: COMPONENT_GET_DEADLINE_EXCEEDED` with `retryable: true`, which are contract fields, not
prose. But the evidence is not uniform: other steps distinguish "Boomi rejected it" from "Boomi
committed it and the reply was lost" only by a Python exception class name, and a 200 carrying
unexpected XML lands in the rejection bucket. A rule that is safe for some steps and silently
wrong for others is worse than one that is uniformly conservative, so `possible` covers all of them.

The consequence is stated rather than hidden: a step that provably wrote nothing — the GET-timeout
case above — is still reported `possible` with `mutation_performed: true`, and still withholds the
retry code. Its own `retryable: true` survives untouched inside `partial_results[<key>].result`, so
the information is available to a caller who wants it; the top-level signal simply does not claim
more precision than the surface can guarantee. Narrowing this properly means having the materializer
record whether it dispatched a write, so the predicate reads a fact instead of inferring one — a
change to the builder layer, and its own issue.

## 3. Terminology — four graphs, four names

A single word for four different structures is how a reader ends up believing the wrong one is
executable. Every response this issue introduces uses:

| Name | What it is |
|---|---|
| `pipeline_stages` | the inert `PipelineSpec` echo (ADR-001 §5) — drives nothing |
| `process_cfg` | the compiler's semantic control-flow graph, summarized shape-only |
| `component_dependencies` | ComponentPlan materialization edges |
| `topology_relations` | `SystemTopologySpecV1` relations — each carries `relation_kind`, `relation_key` and typed `participants` (`role` + `ref`), derived from the variant's own fields. Relations are not uniformly binary (`deployment_binding` binds three objects), so there is deliberately no source/target pair. |

`IntegrationSpecV1.flows` and the `flow_sequence` process-config key keep their names. They are
frozen legacy surface (ADR-001 §9); renaming them is not in this issue's scope, and the rule above
binds the names #146 introduces.

## 4. Revisions and the binding

Four related but **non-interchangeable** digests, all spelled `sha256:<64 lowercase hex>`:

| Digest | Covers |
|---|---|
| `schema_revision` | every authoring schema `get_schema_template` serves |
| `capability_revision` | the whole capability manifest, including `schema_revision` |
| `semantic_hash` | one normalized, secret-free authoring intent |
| `plan_hash` / `compile_hash` | that intent bound to its planning and compilation evidence |

`compiler_revision` is exposed separately and identifies compiler + validator behavior.

Revisions are **derived, never declared** — computed from the live archetype registry, the live
recipe registry snapshot, and the runtime models' own JSON Schemas. A hand-maintained version string
would drift from behavior exactly the way the four-vs-six catalog did. They are deliberately *not*
source hashes or git SHAs: equivalent packaged code must produce the same revision, or a
rebuilt-but-identical deployment would report drift against itself.

`account_scope_hash` is a one-way hash whose scope is the **account**, not the profile name. A
profile is an alias: two profiles can address one account, and one profile can be repointed at
another. It falls back to the profile name only when the account id is unavailable, and records
which of the two it used so the fallback cannot collide with a real account scope. It is a field of
the revision binding, the binding is hashed into `plan_hash`, and `plan_hash` is hashed into
`compile_hash` — so a binding minted against one account cannot satisfy an apply against another,
and neither identifier appears in any response.

### There is no server-side plan cache

A cached plan token is authoritative only on the instance that minted it and only until it is
evicted. Apply **recomputes and compares**. That survives a restart, works across instances, and is
why `_BUILD_REGISTRY` being session-scoped does not weaken the binding.

## 5. Honest comparison

`compare_capability_revision(None)` returns **`not_requested`** — never `match`. A caller who
supplied no expectation has verified nothing, and reporting parity they never asked for is the same
dishonesty as hiding a mismatch. The vocabulary (`not_requested` / `match` / `mismatch` / `unknown`)
is the one `RecipeRegistrySkewV1` already ships on the sibling recipe surface.

A mismatch still returns the **full live catalog**, so the caller can see *what* differs rather than
only *that* something does.

Recovery from drift or staleness: re-run `list_capabilities`, re-fetch the schemas, re-plan,
recompile, then apply with the new binding.

Verify reports `unknown` — never `match` — when any build-owned component has no apply-time
baseline to compare against (its read-back failed when the build was created). Those components are
listed in `unverifiable_components`, so "not compared" can never be mistaken for "unchanged".

## 6. Capability gaps — what this surface cannot do, said out loud

Two capabilities are published as `unsupported` rather than omitted. An absent capability is
indistinguishable from one the client forgot to ask about.

| Capability | State | Reason |
|---|---|---|
| `authoring.system_topology.deploy` | unsupported | `TOPOLOGY_APPLY_NOT_SUPPORTED` — #144 ships a planner and no apply path at all |
| `authoring.typed_apply.process_materialization` | unsupported | `PROCESS_KIND_REQUIRED` — see below |

### An intent that compiles a ProcessIR root is plan/compile-only

Boomi processes are created by the legacy builders, which emit XML from `config.process_kind`.
Nothing on a production path materializes a ProcessIR root: the compiler stops at the emission plan,
and promoting its emitter to a production component writer is an ADR-001 §9 byte-parity cutover that
belongs to its own issue.

A `process_kind` on the component would not rescue it. Adding one would make things worse, not
better: the builder would emit XML from the component config while
the binding attested to the ProcessIR emission plan, so the compile hash would certify an artifact
that was never created. A binding that means what it says is the whole point of this milestone, so
this is refused — and the gap is reported at **plan** time, before a caller spends a compile.

The rule keys on the **compiled artifact**, not the intent kind: if compilation fingerprinted a
ProcessIR root, apply must materialize that root. That covers a direct `process_ir` intent AND a
`recipe` intent whose composed roots were compiled — refusing one while permitting the other made
the safety argument incoherent, since both certify one representation and build another.

`integration_spec` is unaffected: it produces no process roots, so its binding never claims a process
artifact and apply is the legacy behaviour plus a binding over the component plan.

### Artifact fingerprints describe the emission plan, not XML

The canonical compile step's deterministic, already-golden-tested output is what gets fingerprinted.
Live drift is a different comparison entirely — **apply-time live component XML against verify-time
live component XML**, both fetched from Boomi. Those two are the comparable pair; comparing a
compile-time digest to live XML would report drift on every healthy build.

## 7. Error taxonomy

ADR-001 §7 reserves the `AUTHORING_*` family to this issue. All seven are `category="authoring_surface"`,
`retryable=False`, `owner="#146"`.

| Code | Raised when |
|---|---|
| `AUTHORING_SCHEMA_VERSION_UNAVAILABLE` | a known selector is requested at a version this server does not serve |
| `AUTHORING_CAPABILITY_REVISION_MISMATCH` | the caller's expected capability revision differs from this runtime's |
| `AUTHORING_LIVE_DEPLOYMENT_DRIFT` | a build-owned live component no longer matches its apply-time fingerprint |
| `AUTHORING_REQUIRED_DECISION_MISSING` | a required decision is unresolved, or a resolution names a decision this plan never raised |
| `AUTHORING_COMPILE_BLOCKED` | canonical compilation refused the intent; causative canonical codes travel value-free |
| `AUTHORING_PLAN_STALE` | a recomputed hash differs from the one the caller bound to |
| `AUTHORING_APPLY_VALIDATION_REQUIRED` | a typed apply omitted or could not reproduce its compile binding, **or** the compiled plan is not materializable (§6). This code names the phase that refused; the domain reason travels in `cause_codes`. Materializability is checked BEFORE staleness, so an unappliable intent reports this rather than a stale-hash diagnosis it cannot act on. |

A capability-revision mismatch takes **precedence** over a stale plan: when the server's own contract
moved, every downstream hash is expected to differ, and reporting staleness would send the caller to
re-plan against a surface they have not rediscovered.

Canonical codes from the ProcessIR compiler, the semantic validator, the topology planner and the
recipe layer are carried as value-free `cause_codes` — never re-diagnosed. Those taxonomies stay
authoritative about their own domains; this family only names which phase refused.

## 8. Security

The **#146-introduced fields** — diagnostics, gaps, decisions, artifact fingerprints, revision
bindings, resolved references, the CFG summary — carry hashes, opaque references and value-free text
only. They contain no credentials, no headers, no connection properties, no document data and no raw
XML.

That claim does **not** extend to `integration_spec_preview`. It is the caller's own component plan
echoed back through the legacy planner, so it carries whatever configuration the caller authored —
including `base_url`, `headers`, `custom_properties`, and any credential-shaped key outside the
legacy redaction list. The redaction applied is **byte-identical to the legacy `plan` echo** (that
parity was the #146 defect, and it is fixed), but it is the legacy list, with the legacy list's
coverage. See §10.

The distinction is load-bearing rather than pedantic: ADR-001 §11 scopes its prohibition to ProcessIR
and topology documents, fixtures, logs and diagnostics — it says nothing about the `IntegrationSpecV1`
echo, so citing it for the preview would be claiming an authority that does not cover it.

The capability manifest additionally excludes absolute paths, timestamps, pids, build
ids, profile names and raw account ids; provenance is symbolic (`runtime_schema_registry`,
`archetype_registry`, `recipe_registry`, `canonical_compiler`, `semantic_validator`,
`topology_planner`) and never a filesystem path.

The one place an opaque caller payload passes through is `RecipeInvocationRequestV1.raw_input`,
which is handed to the recipe engine's own input gate (`RECIPE_INPUT_INVALID`). #146 routes to that
scan rather than duplicating it.

Verify hashes the component XML it already fetched; the bytes never leave the server.

## 9. Migration

* **Legacy callers change nothing.** Tool names, required parameters, the existing `plan`/`apply`/
  `verify` actions, response fields, build ids and verify behavior are unchanged. The
  `build_integration` wrapper signature is byte-identical — the typed input rides inside the existing
  `config` JSON precisely so that claim can be made.
* **Typed authoring is opt-in**, selected only by an explicit `config.authoring_request`. A legacy
  request is never reinterpreted as a typed one.
* `config.authoring_request` is **mutually exclusive** with the legacy `integration_spec` /
  `source_description` / `components` roots. Sending both is rejected rather than resolved by
  precedence — a precedence rule silently ignores one payload, and the caller cannot tell which.
* **Revision bindings are required only on a typed apply.** Plan and compile accept them as optional
  staleness checks.
* **`compile` is additive.** Nothing was removed or renamed.
* **No deprecation schedule in M12.11.** Nothing is deprecated and nothing warns.

## 10. Known limits

* `_BUILD_REGISTRY` is a process-global, session-scoped dict. `verify` after a server restart
  already returned `Unknown build_id` before this issue, and authoring provenance inherits that
  limit. The binding does not depend on it — apply recomputes rather than looking anything up.
* Plan and compile resolve references read-only and run the legacy component-plan lint, which costs
  metadata queries per named component. That is the price of the `version_marker` evidence that makes
  staleness detectable and of the duplicate-connection / base-URL / folder / name warnings. When no
  authenticated client is available the lint is reported as **unknown**, never as clean.
* An unresolvable `$ref` is **not** caught by semantic validation (reference resolution is not one of
  its phases) — it is caught at compile, when the emission plan is built. Since a typed apply runs
  through compile, an unresolvable reference cannot reach a mutation.
* The component-plan lint's result is part of the plan evidence and therefore of the plan hash, so a
  binding compiled without an authenticated client cannot satisfy an apply made with one. The tool
  layer always supplies a client, so both sides agree in practice; the library-level default is
  pinned by a test so the difference is a decision rather than a surprise.
* **Credential redaction is inherited, not widened.** The typed path now produces byte-identical
  redaction to the legacy path — that parity was the #146 defect and it is fixed. The underlying
  legacy redaction is an exact-match list of a handful of field names, so credential-shaped keys
  outside it (`api_key`, `refresh_token`, `passphrase`, camelCase spellings, …) still echo on
  **both** paths. That is pre-existing behavior shared with every existing caller; widening it
  changes the legacy contract and belongs to its own issue, not to #146 (recorded from live QA as
  bug #413).


## 11. Deviations from the architect design plan, and what remains open

The §6 architect-vs-plan review found the implementation faithful in intent but narrower than the
plan in specific, named ways. Recorded here rather than left implicit — a reduction nobody wrote
down is a reduction nobody can approve.

### Accepted deviations

**No route both canonically compiles AND materializes the same artifact.** The plan envisioned one
end-to-end workflow. What ships is: `process_ir` and `recipe` intents compile canonically and are
refused at apply (§6); `integration_spec` applies through the legacy builders and produces no
process artifact. Closing this needs a production ProcessIR materializer with byte-parity against
the legacy renderer — an ADR-001 §9 cutover, and its own issue. **This is a scope reduction that
needs explicit approval, not a defect.**

**Account scope is keyed on the account, not the profile — a WEAKER property than the plan asked
for, recorded here for approval.** The plan asked for cross-*profile* replay prevention. Live QA
measured the cost of that: two profiles addressing one account produced different bindings, so a
valid compile was refused with a false "stale plan" diagnosis.

An earlier version of this note justified the change by claiming two profiles for one account "carry
the same authority". **That is false** and the claim has been withdrawn: a profile stores its own
username and password, so two profiles on one account can be different principals with different
Boomi roles. What is actually true is narrower — the binding is a *staleness and integrity* check,
not an authorization token. Boomi's own authorization still governs what each credential may do, and
a typed apply recompiles under the ACTIVE profile before its first write, so a replayed binding
cannot make profile B perform something B is not permitted to perform.

The residual gap is real and is the thing to approve or reject: **a compile produced under profile A
can be applied under profile B when both address the same account.** If that is unacceptable, the
fix is to put the profile back into `account_scope_fingerprint` and accept that two profiles for one
account can no longer share a binding.

**The typed recipe intent carries invocations, not the #145 contribution union.** A recipe's
`raw_input` is recipe-defined and open by construction; the engine's own input gate is what
validates it. Embedding the closed contribution union would have described what recipes *return*,
not what a caller *sends*.

### Known limits, not fixed here

- **The typed contract is not secret-free at the type level.** `IntegrationComponentSpec.config` is
  `Dict[str, Any]`; it is hashed into `semantic_hash` and echoed through `integration_spec_preview`
  under the *legacy* redaction, which this issue achieved byte-parity with but did not widen. A
  typed-only allowlisted projection would close it without touching legacy behaviour.
- **Topology is validated, not planned.** `_validate_topology` calls `validate_system_topology` and
  not `plan_system_topology`. Blocking topology findings ARE returned, as authoring diagnostics, and
  their codes and counts enter the validation summary that is hashed into `plan_hash` — so they are
  surfaced and partially bound. What is omitted is the rest of `SystemTopologyPlanV1`:
  `capability_report`, `resolved_references`, `executable_component_prerequisites`,
  `planning_only_relations`, `runtime_process_order`, `guidance`, the typed `blockers` records and
  `unresolved_decisions` — plus topology validation *advisories*, which are discarded entirely (only
  errors and warnings become authoring diagnostics). `required_decisions` is
  consequently always empty — the machinery exists and is tested, but no decision family populates it.
- **Verify compares component XML, not dependency edges or reference versions.** ~~and checks the
  capability revision but not the compiler revision.~~ **The compiler-revision half is CLOSED by the
  #146 amendment**: `compiler_revision` now covers the body-placement rows, the connector capability
  rows, the replay-safety rules, the state-visibility model, the process-property scope, all three
  diagnostic spec tables and the served authoring projection — so a behaviour change moves it. The
  XML-versus-edges limit stands.
- ~~**`schema_revision` covers selector-to-body hashes, not selector+version pairs.**~~ **CLOSED by
  the #146 amendment.** Owned selector versions now participate in `schema_revision` directly, so a
  version bump moves it even when the body is byte-identical. Separately,
  `cache_property_authoring` was absent from the inherited selectors *and* published no recognized
  schema body, so a change to the state-visibility semantics moved nothing at all; it is now
  inherited with a manifest-free digest, and a visibility change moves `schema_revision`. Inherited
  selectors still publish no version of their own and still reject the `@<version>` suffix.

- **`build_integration` keeps its three-parameter signature.** The typed request rides inside the
  existing `config` JSON rather than becoming strict wrapper parameters. That was chosen so the
  wrapper signature stays byte-identical — the strongest backward-compatibility statement available
  — at the cost of the plan's "one unified strict contract" across all five wrappers.
