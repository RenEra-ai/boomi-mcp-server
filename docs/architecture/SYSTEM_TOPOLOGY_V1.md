# SystemTopologySpecV1 — capability-gated topology planning

**Status:** landed, DARK (no MCP surface). **Owner:** issue #144 (M12.9), epic #134.
**Depends on:** #135 (ADR-001 authority/capability policy), #136 (`ProcessIRV1` process references).
**Successor:** #146 owns the eventual MCP planning surface. Any apply/deploy support requires a
separate, evidence-backed issue.

`SystemTopologySpecV1` is the planning-only authority ADR-001 §3 reserves for #144: *"a future
capability-gated, planning-only topology authority (#144). It never mutates runtime state and never
feeds the process compiler."*

It is **dark**: nothing at runtime constructs or consumes it, no MCP tool or action is registered,
and no existing schema or behavior changed.

---

## 1. Three authorities, three graphs

| Authority | Owns | Graph | Namespace marker |
|---|---|---|---|
| `ProcessIRV1` (#136) | semantics *within* one process | CFG edges | compiler-internal |
| `IntegrationSpecV1` | component materialization | build dependencies (`depends_on`) | `owner="component_plan"` |
| `SystemTopologySpecV1` (#144) | relationships *between* processes and resources | ProcessCall runtime order | `namespace="system_topology"` / `"topology_runtime"` |

These are **siblings, not layers**. `boomi_mcp.compiler.system_topology` does not import
`boomi_mcp.compiler.process_ir`, and vice versa.

The separation is enforced three independent ways, not asserted:

1. **Vocabulary** — the authored schema declares no field a caller could use to express a build
   dependency or a CFG edge (`tests/test_system_topology_graph_namespaces.py`).
2. **Import isolation** — a source scan plus a fresh-subprocess module ledger.
3. **Byte independence** — perturbing the build graph leaves the runtime order byte-identical, and
   perturbing the call graph leaves the prerequisites byte-identical. This is the proof that would
   catch a real leak; the first two catch the ways one usually gets introduced.

---

## 2. The authored contract

Root: `SystemTopologySpecV1(version="1", profile_ref, objects[≥1], relations[])`.

`profile_ref` is a single document-level fact, so a cross-profile reference is **unrepresentable**
rather than merely rejected.

### Objects (closed discriminated union, 10 kinds)

| `kind` | Fields | Notes |
|---|---|---|
| `process` | `key`, `component_ref` | `$ref:KEY` = planned; literal id = existing |
| `api_service` | `key`, `component_ref` | Boomi `webservice` (API Service Component) |
| `document_cache` | `key`, `component_ref` | |
| `process_property` | `key`, `component_ref` | |
| `runtime` | `key`, `runtime_ref` | atom/molecule/cloud — what a schedule binds to |
| `environment` | `key`, `environment_ref`, `classification?` | `TEST`/`PROD`, optional |
| `schedule` | `key` | intent marker; identity comes from its binding |
| `deployment_unit` | `key` | non-executable grouping; identity comes from its binding |
| `external_queue` | `key`, `resource_ref` | **gated** |
| `external_event_stream` | `key`, `resource_ref` | **gated** |

### Relations (closed discriminated union, 8 kinds)

| `kind` | Roles |
|---|---|
| `process_call` | `caller_process` → `callee_process` |
| `api_service_route` | `api_service` → `listener_process` |
| `document_cache_use` | `process`, `document_cache` |
| `process_property_use` | `process`, `process_property` |
| `schedule_binding` | `schedule`, `process`, `runtime` |
| `deployment_binding` | `deployment_unit`, `process`, `environment` |
| `queue_reference` | `process`, `external_queue` — **gated** |
| `event_stream_reference` | `process`, `external_event_stream` — **gated** |

**Role-named fields, not generic endpoints.** A generic `(source, target)` pair would make
`ProcessCall(A, B)` and `DeploymentBinding(unit, env)` the same shape, so only convention would stop
an environment being put where a runtime belongs. Naming the roles makes the endpoint matrix a *type
fact*, which is what lets `TOPOLOGY_REFERENCE_TYPE_MISMATCH` be decided structurally.

**Ternary bindings.** A schedule's platform identity *is* the `(atom, process)` pair, so splitting it
into two edges would permit a half-bound schedule the platform cannot represent. Likewise a
deployment unit takes exactly one process and one environment.

### What the schema rejects

Every authored boundary is `extra="forbid"`, and a recursive pre-scan runs **before** model
validation, rejecting:

- **secret-shaped keys** (substring match, ProcessIR's list plus `certificate`,
  `environment_extension`, `profile_override`, `connection_propert*`);
- **open-payload keys** (exact match): `config`, `configuration`, `metadata`, `xml`, `raw_xml`,
  `component_xml`, `extensions`;
- **derived verdict keys** (exact match): `capability`, `provenance`, `evidence`, `action`, `apply`.

That last group is the gate's premise. If a spec could carry `capability: "emittable"`, the registry
would be advisory — evidence decides, not the caller.

---

## 3. Capability states and provenance

| State | Meaning |
|---|---|
| `emittable` | A typed ComponentPlan materializer can produce the prerequisite, after separate build authorization. The planner **reports**; it never calls a materializer. |
| `plannable-only` | Source/docs plus a trusted witness support reference, lifecycle and ordering validation. **No apply path.** |
| `guidance-only` | Evidence supports advice or an unresolved decision only. Cannot enter an executable or planning bucket. |
| `gated-no-evidence` | Worth representing as intent, but required evidence is missing. Always a blocker. |
| `unsupported` | Policy or platform rejects it. Absent from the schema, or a named diagnostic. |

Every subject carries **three evidence legs** — `source`, `documentation`, `live` — each with its own
status: `verified`, `corroborating_only`, `unavailable`, `conflicting`, `not_captured`. Three
separate legs is what makes the issue's "where available" checkable rather than rhetorical: a
subject whose live leg is `unavailable` is visibly different from one nobody looked at
(`not_captured`) and from one whose sources disagree (`conflicting`).

**A `conflicting` leg is reported, never resolved by precedence.** Picking a winner would hide
exactly the case a human needs to see.

Absence is denial: an unregistered subject raises at import (`_validate_coverage`), checked against
both discriminated unions in both directions.

---

## 4. Current evidence — and six corrections to the issue's own text

The issue body's "Current evidence" section is partly stale. The live read that grounds this
contract corrects it in six places, and each correction weakened a claim rather than strengthening
one.

| # | Issue said | Live evidence | Consequence |
|---|---|---|---|
| 1 | "85 processes, 100 Document Cache components, 81 Process Property components" | Counts are **page-capped artifacts**: `documentcache` returned `total_count: 100, has_more: true, total_available: 186` | No census number is hard-coded anywhere. Every live query records `returned_count`, `total_available`, `has_more`, and derived `truncated`. |
| 2 | "Listener/API-service relationships are split between component dependencies and free-form metadata" | **Zero `webservice` components in *either* live profile.** The M6.1/#133 ASC fixtures are gone. | `api_service` stays `emittable` (the typed builder is real) but its **live leg is `unavailable`**. `api_service_route` needs a typed-builder or parsed-XML witness. |
| 3 | "Live representative processes include ProcessCall" | `analyze_component(action="dependencies")` returns a **flat, one-level, mixed-type list with no edge kind** — 6 processes, 1 `profile.json`, 1 `profile.flatfile`, 1 `documentcache`, all identical in shape | The dependency API **cannot witness a ProcessCall**. It is `corroborating_only`, typed as `DependencyCorroborationV1` so promoting it is a type error. `dependency_api_as_process_call_witness` is registered `unsupported`. |
| 4 | "six inactive schedules" | All six carry an **empty `schedules: []` body**; the id is base64 of `CPS{atomId}:{processId}` | Schedule **content** has zero evidence → `guidance-only`, absent from the schema. A schedule binds a process to a **RUNTIME**, never an environment → `schedule_environment_binding` is `unsupported`. |
| 5 | "zero active deployments" | **False as stated.** `work` has 18 inactive records; `renera` has an **active** one. | Deployment reads establish that records exist *and can be listed*; they establish nothing about **creating** one. `topology_apply` and `atomic_multi_process_deployment` stay `unsupported` — the verdict never depended on the active flag, and a reason falsifiable by looking at a second account is not a reason to publish. |
| 6 | "zero queue components" | Confirmed, both profiles | `external_queue`, `external_event_stream`, `queue_reference`, `event_stream_reference` are permanently `gated-no-evidence` in V1. ADR-001 §12 rejects speculative queue mutation outright. |

Two further gaps have no capture at all:

- **`account_capability_limits`** — `not_captured` on all three legs. No limit field is modeled, and
  every plan carries an unresolved decision saying so.

A seventh property is enforced rather than observed: **an unanswered query is not a zero.**
`DiscoveryPageProvenanceV1.observed` separates "the query ran and found nothing" from "the query did
not answer", and an unobserved page is fail-closed to `truncated` and raises its own
`discovery_unobserved_query` decision. Without that split, a transient outage would be recorded as a
confident zero — and all four queue/Event Streams gates cite an empty listing as their live evidence.

Likewise, **a missing environment classification is never defaulted.** `EnvironmentFactV1.classification`
is optional and set only when the source carried a value: defaulting it to `TEST` would let an author
correctly declaring `PROD` be contradicted by a value nobody read, which is the opposite of the
collector's rule that only a genuine contradiction is a finding.
- **`listener_status_as_api_route_witness`** — the source's observed behavior conflicts with its own
  documented example, so it is `unsupported` with `conflicting` source and documentation legs.

**No environment classification is assumed.** One of the two live profiles has two `TEST`
environments and no `PROD`; requiring one would make a real account unmodelable. `classification` is
optional, and authoring it opts into an equality check against discovery.

---

## 4b. Evidence discipline in the resolver

Six rules the resolver enforces, each of which the first implementation got wrong in the
same direction — by treating a gap in evidence as a fact.

**Type is carried, never discarded.** Indexes hold `(id, type)`, not bare ids, and a reference
resolves only when the backing component is the type the object kind requires
(`process`→`process`, `api_service`→`webservice`, `document_cache`→`documentcache`,
`process_property`→`processproperty`). An id-only index resolves a `process` object pointing at a
Document Cache exactly as happily as a correct one.

**Absence and mismatch are asymmetric.** A wrong type is conclusive from the fact alone — we read
the component and it is a cache. Absence is conclusive only when the listing that would have
contained it was **observed and complete**: a literal id missing from a 100-of-186 page is not
evidence the component does not exist. A ComponentPlan symbol table is complete by construction (it
*is* the plan); a live listing is not.

**Profile isolation is per FACT.** A snapshot can carry the right profile while an individual fact
inside it names another account. Such facts are **discarded** before indexing — a fact from another
account is not weaker evidence about this one, it is evidence about a different system — and their
presence raises `TOPOLOGY_ENVIRONMENT_MISMATCH` rather than being dropped silently.

**Runtimes are not an inventory.** Discovery derives runtimes from *schedule rows*, so it sees only
runtimes that already have a schedule. `runtime_inventory_complete` is `False` and nothing sets it
True; absence of a runtime therefore witnesses nothing. Treating that list as authoritative would
report every unscheduled runtime as not-found and block the primary use case — binding the **first**
schedule to a runtime.

**A blocked endpoint withdraws its relation.** An endpoint's failure is reported under `/objects/N`,
a different path from the relation's own, so a filter keyed on `/relations/N` alone left a
structural binding in `planning_only_relations` while it pointed at an unresolvable object. A plan
may not present a relation as plannable when one of its ends is blocked.

**Cycles are located by strongly connected component**, not by pruning sources and sinks. Two cycles
joined by an acyclic bridge leave every node with both an in-edge and an out-edge, so nothing prunes
and the *bridge* survives as if it were cyclic — and removing it breaks neither cycle. The pointer
is the lowest authored index among edges **internal** to a cyclic SCC (iterative Tarjan, so a deep
call graph cannot blow the stack).

---

## 5. Witness policy

What counts as proof depends on whether the process **exists** yet:

| Process form | Accepted witness | Why |
|---|---|---|
| planned (`$ref:KEY`) | a matching `ProcessIRV1` node | There is no component to parse; the IR root is the authority for what it will do. |
| existing (literal id) | the component's own XML | An authored IR may describe an intended future shape rather than what is deployed. Accepting it would let a plan assert an edge the deployed component does not have. |

`project_process_ir_evidence` silently contributes nothing for a literal id — the fail-closed
outcome, since the relation is then gated, which is the correct verdict.

**The rule holds at the point of USE, not only at manufacture.** `evidence.py` enforces it when it
constructs witnesses, but `TopologyResolutionContextV1` is a public input: a caller can hand over
`ProcessCallEvidenceV1(witness="process_ir")` for a literal id directly and bypass the constructor
entirely. `collect_lifecycle_findings` therefore re-checks the witness against the subject's form —
planned subjects accept `process_ir`/`typed_builder`, existing subjects accept `component_xml` — and
gates the relation on a mismatch in either direction.

Extraction **parses** the document and walks elements; it does not pattern-match text. That
distinction is load-bearing: a regex has no idea what a comment is, so
`<!-- <processcall processId="x"/> -->` produced a witness for an edge the process does not have —
and a witness is what authorizes a planning relation. `iter()` yields elements only, so a
commented-out shape contributes nothing without any special handling. Tags and attributes are matched
namespace-stripped and case-insensitively, because Boomi XML carries namespaces.

Everything fails closed: oversized, DTD-bearing (`DOCTYPE`/`ENTITY` are rejected before parsing —
XXE and billion-laughs, mirroring `categories.schema_discovery._safe_xml`), or malformed all yield
*no witness*, and a relation with no witness is gated — the correct verdict for a document we could
not read.

Raw XML never leaves `evidence.py`: parsing extracts typed witness rows and discards everything else,
so a connection property sitting beside a `<processcall>` cannot ride along. Route witnesses carry no
path, method, or endpoint configuration, and require a real WSS listen element rather than text that
merely mentions one.

---

## 5b. Corrections from the architect-vs-plan review

A second Codex review judged the implementation against the design plan rather than on its own
merits, and found seven places where the code had drifted from what the plan specified. Each is
recorded because each was a real capability the contract claimed and did not have.

| # | Gap | Correction |
|---|---|---|
| 1 | `has_process_ir` was carried on every ComponentPlan symbol and consumed by nothing, so a `witness="process_ir"` label was trusted on its own word | A ProcessIR witness now requires the planned symbol to declare a ProcessIR root. There was nothing for the claim to be true *of*. |
| 2 | `parse_api_service_component_evidence` searched for `<wss>` inside the ASC's own XML. A real ASC has none — the WSS Listen lives on the linked **process's** start shape | Restored the plan's three-argument shape: process-side listen confirmation is threaded in, so an existing-ASC route can be witnessed at all. |
| 3 | Per-fact profile filtering anchored on `snapshot.profile`, itself caller-supplied | Anchored on the **context's** profile. A snapshot stamped with the wrong profile previously kept every fact inside it — the one arrangement the filter exists to stop. |
| 4 | Document rules (duplicate keys, duplicate semantic relations, unbound schedule/deployment units) ran only in `parse_system_topology_v1` | They now run as the planner's `model` phase. A caller who built the spec with `model_validate` got **no duplicate-key error at all**, failing an acceptance criterion on the planner's own surface. |
| 5 | Invalid or unresolved subjects reached the executable and planning buckets: a prerequisite emitted after a type-mismatch blocker; an empty environment inventory waving every environment through; `witness="live_fact"` claimed with no snapshot | Blocked objects yield no prerequisite; an empty `list_environments` result is conclusive (that read is not paged); and a structural binding is labelled `declared_intent` unless a snapshot genuinely corroborates it. |
| 6 | The port declared `read_component_xml` and `read_component_dependencies`; the capture invoked neither | `capture_existing_component_evidence` performs both. Without it, every literal-id ProcessCall, cache use, property use and API route was gated regardless of what the account contained. |
| 7 | Diagnostic contract details diverged: unknown discriminators pointed at the member position rather than `/kind`; a doubled schedule/deployment binding reported an unsupported *lifecycle* rather than a *cardinality* violation; the normative `lifecycle` phase was unused, with witness failures folded into `relation` | All three aligned. Witness-level gating now uses `lifecycle`; kind-level gating keeps `capability`. |

Three additions beyond the plan's enumerated surface are deliberate and are recorded here rather
than removed: `SystemTopologyPlanV1.validation` (the only path by which warnings and advisories
reach a caller — `guidance` is derived separately), `TopologyPlanningInvariantError` exported from
the package root (a caller must be able to catch it), and the derived `TOPOLOGY_OBJECT_KINDS` /
`TOPOLOGY_RELATION_KINDS` / `TOPOLOGY_RELATION_ROLES` reflection tables (derived from the unions, so
they cannot drift). The package export list is pinned exactly.

`ScheduleBindingFactV1.observed_max_retry` is likewise an addition: every live schedule carried
`retry.max_retry: 5` while its `schedules` body was empty, so it is the one piece of schedule
configuration with live evidence. It is recorded as an **observation** on the snapshot and appears
nowhere in the authored contract.

---

## 6. The plan

`plan_system_topology(spec, context, requested_operation="plan") -> SystemTopologyPlanV1`

Phase order is fixed and normative:

```
model → capability → reference → relation → lifecycle → environment → dependency → plan_invariant
```

Findings **accumulate** — a caller fixes everything in one pass. Only the dependency phase depends on
an earlier one: a cycle report over unresolved references would name relations that resolve to
nothing.

### Buckets

| Bucket | Rule |
|---|---|
| `executable_component_prerequisites` | `$ref`-backed objects whose symbol is present. A literal id names something that already exists — reporting it as something to build would be an instruction to rebuild a live component. |
| `resolved_references` | Everything that resolved, and to what (`component_plan_symbol` / `existing_component` / `platform_resource`). |
| `planning_only_relations` | Witness-backed, non-gated relations that survived every phase. |
| `runtime_process_order` | Lexically tie-broken Kahn ordering over **`process_call` only**, callee before caller. A blocked plan reports the empty order. |
| `guidance` | `warning`/`advisory` findings and guidance-only feature rows. |
| `blockers` | Exactly the `error` bucket. |
| `unresolved_decisions` | Questions a human must answer or evidence must settle. Carries no severity — "we do not know yet" is not on a scale with "this is wrong". |

**Severity → bucket:** only `error` blocks. `warning`/`advisory` reach `guidance`.

**A blocker does not suppress the rest of the plan.** One gated queue must not make an
otherwise-complete plan look empty.

### `apply_supported: Literal[False]`

A **type**, not a runtime value. A plan asserting it is appliable is unconstructible, so no code
path, test double, or future edit can produce one by flipping a flag.

`requested_operation != "plan"` is refused **before the context is read at all** — proven with a spy
context that raises on any attribute access.

### Determinism

Canonical serialization is `model_dump(mode="json")` → `json.dumps(sort_keys=True,
separators=(",", ":"), ensure_ascii=True)`. Every derived collection has an explicit **total** order;
stopping short of one leaves items in an order that is stable within a process and unstable across
runs — the exact defect the criterion exists to prevent. Pinned across `PYTHONHASHSEED` values in
separate subprocesses, since the seed is fixed at interpreter startup and an in-process loop cannot
vary it.

### Internal defects vs authored problems

`check_topology_plan_invariants` re-verifies every claim the contract makes. A failure raises
`TopologyPlanningInvariantError` — **not** a `TOPOLOGY_*` diagnostic. Blaming the caller for our bug
is how someone ends up rewriting a correct payload to route around it; the same reasoning walls
`PROCESS_IR_COMPILE_*` off from a `ValidationReportV1`.

---

## 7. Error family

ADR-001 §7 reserves `TOPOLOGY_*` to #144, which is its **sole** introducer — asserted as a
biconditional in `tests/test_error_taxonomy.py`. **Fourteen codes**, all `category="topology"`,
`retryable=False`, `owner="#144"`.

| Code | Trigger |
|---|---|
| `TOPOLOGY_SCHEMA_UNKNOWN_OBJECT` | missing/unknown object discriminator |
| `TOPOLOGY_SCHEMA_UNKNOWN_RELATION` | missing/unknown relation discriminator |
| `TOPOLOGY_SCHEMA_UNKNOWN_FIELD` | any extra, secret-shaped, open-payload or derived-verdict field |
| `TOPOLOGY_SCHEMA_INVALID_CARDINALITY` | empty object list; unbound schedule or deployment unit; a schedule or deployment unit bound more than once |
| `TOPOLOGY_SCHEMA_DUPLICATE_KEY` | duplicate object/relation key, or duplicate semantic relation tuple |
| `TOPOLOGY_SCHEMA_VERSION_UNSUPPORTED` | missing/non-`"1"` version |
| `TOPOLOGY_SCHEMA_INVALID` | remaining strict type/shape/reference-format failure |
| `TOPOLOGY_REFERENCE_NOT_FOUND` | role or reference unresolvable in this profile's context |
| `TOPOLOGY_REFERENCE_TYPE_MISMATCH` | role resolves to a kind it does not accept |
| `TOPOLOGY_RELATION_UNSUPPORTED` | scheduled listener; process self-call |
| `TOPOLOGY_CAPABILITY_GATED` | gated kind present (phase `capability`), or supported relation with no witness (phase `lifecycle`) |
| `TOPOLOGY_ENVIRONMENT_MISMATCH` | profile disagreement, or contradicted environment classification |
| `TOPOLOGY_DEPENDENCY_CYCLE` | ProcessCall cycle, pointed at the canonical earliest participating relation |
| `TOPOLOGY_APPLY_NOT_SUPPORTED` | any `requested_operation` other than `plan` |

Every diagnostic carries a stable code, an RFC 6901 JSON pointer into the **authored** payload, a
static message and static remediation. Nothing is interpolated — `topology_finding()` deliberately
has no `message` parameter, because once one exists every future call site is a place an authored
value can leak into a log line. `subject` and `provenance` are constrained to structural tokens.

**`/operation` is a request-envelope pointer**, not an authored JSON path: `requested_operation` is a
function argument with no corresponding spec field. ADR-001 §7 already makes this accommodation for
the `M12_*` family, where the audited scope stands in for the authored path.

---

## 8. Non-mutation guarantee

"No mutating/executing MCP tool is reachable from this issue's topology planner" is proved
behaviorally, not by grep:

- A real plan runs in a fresh interpreter under `sys.addaudithook`, which rejects socket creation,
  subprocess launch, `os.system`/`exec`/`spawn`, environment mutation, file removal/rename/mkdir,
  `shutil` moves, and `open` **in a write mode** (the `open` event fires on reads too, so rejecting
  it unconditionally would make the test pass while proving nothing).
- A **positive control** in the same process under the same hook attempts a socket and asserts the
  hook fired — otherwise a hook that never fires is indistinguishable from a clean plan.
- The hook is installed **after** all imports: it cannot be removed, and import machinery
  legitimately opens files.
- A module ledger confirms no `server`, SDK, HTTP, execution, deployment, schedule, environment,
  listener, shared-resource or troubleshooting module loads during a plan — with its own positive
  control.
- The context models are walked for `Any`/`Callable`/`Dict`/client/action/config/XML annotations: a
  planner that cannot be *given* a client cannot call one.
- No topology tool is registered on the MCP surface, and importing `server` does not load the
  planner.

The discovery boundary is a seven-method `typing.Protocol` — structural, unregistered, no ABC
registry, no metaclass, no SDK import — with no shipped adapter. Every method takes `profile`
explicitly; a port that remembers its profile can be handed to a capture for a different one and
silently answer from the wrong account. An unknown profile is a hard error, never an empty snapshot:
an empty snapshot reads as "this account has nothing", which would turn a typo into a confident
claim that no queues exist.

**Deliberately absent from discovery:** account limits (no capture exists), listener status
(conflicting evidence), environment extensions (they carry override *values*), shared web-server and
channel resources (unrelated, secret-bearing, mixed write verbs), runtime queue listings (a
troubleshooting surface — runtime introspection is not authoring evidence).

---

## 9. Test map

| Requirement | Suite |
|---|---|
| Models/schema for every kind; forbidden fields | `tests/test_system_topology_models.py` |
| Capability states and evidence provenance | `tests/test_system_topology_capabilities.py` |
| Reference/type/cycle/environment/capability negatives | `tests/test_system_topology_validation.py` |
| Deterministic plan snapshots; multi-process fixture | `tests/test_system_topology_planner.py` |
| Read-only discovery, profile isolation, redaction | `tests/test_system_topology_discovery.py` |
| Runtime-edge namespace separation | `tests/test_system_topology_graph_namespaces.py` |
| No mutating/executing tool reachable | `tests/test_system_topology_no_mutation.py` |
| Existing build/deploy behavior unchanged | `tests/test_system_topology_regressions.py` |
| `TOPOLOGY_*` family ownership and census | `tests/test_error_taxonomy.py` |

Goldens live in `tests/fixtures/system_topology/` and use placeholder references only — no real
account id, secret, endpoint value or raw XML.

Run with `PYTHONPATH=src .venv/bin/python -m pytest`.
