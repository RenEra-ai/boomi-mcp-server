# Typed Executable Recipe Contributions V1

> Issue #145 (M12.10) · Milestone M12 · Parent epic #134
> Depends on #136 (ProcessIRV1), #139 (canonical parity path), #144 (SystemTopologySpecV1)

## 1. What this adds, and what it deliberately does not

The repository mixes three kinds of thing that all look like "a pattern": executable
archetype/composition code, advisory doctrine prose, and compatibility adapters that
accept legacy free-form input. Before this issue there was no way to tell them apart
mechanically, so the compiler could not prove what semantics a given surface added or
which constraints applied to it.

This adds a **recipe layer**: registered code with a strict input model that may emit
only four typed contribution values, all of which pass the *same* validators as direct
authoring. Doctrine stays advisory and becomes structurally non-executable.

It does **not** add: doctrine execution, LLM-authored Python, JSON Merge Patch, raw XML,
free-form config, credentials, caller-authored graph edges, topology mutation, a runtime
registration API, or any weakening of an existing validator.

## 2. Surface classification

Every archetype, composition, pattern, planner, guidance and capability surface is now
classified. The full inventory lives in `docs/architecture/M12_COMPATIBILITY_INVENTORY.md`;
the four categories are:

| Classification | Meaning | May execute? |
|---|---|---|
| **executable recipe** | Registered code with a strict input model and declared typed outputs | Yes |
| **constraint-only** | Registered code that may emit `ConstraintRequirement` and nothing else | Yes |
| **advisory** | Doctrine, guidance, planner text. Construction rejects one that declares an executor | **No** |
| **compatibility adapter** | A legacy surface that projects a safe input into an exact recipe version | No — it *calls* one |

The advisory kind is the structural reason doctrine can never become executable. There is
no advisory descriptor a caller can reach that carries one. To be precise about the
mechanism: `RecipeRegistrationV1` is one dataclass for all four kinds, so an `executor`
field does exist — registry construction is what rejects an advisory entry populating it,
with a `ValueError` before the registry is usable. What holds regardless of any field is
that prose is never parsed, so there is no parser to trick.

## 3. The four contribution types

Defined in `src/boomi_mcp/models/recipe_contributions.py`. Every model is
`extra="forbid"`, frozen, versioned (`version: "1"`), repr-redacted, and canonically
serializable. Together they form a closed union tagged by `contribution_kind`, and
`RECIPE_CONTRIBUTION_KINDS` is **derived** from that union — never hand-listed — so the
registry's `output_types` literal cannot drift from it.

### `ProcessIRPatchV1`

Targets the semantic pair `(process_key, closed slot)`. There is no node id, JSON pointer,
or index path anywhere in the model, because **ProcessIR v1 has no authored node identity**:
`label` is optional and non-unique, and CFG/shape ids are compiler-owned. Inventing an
address — an index path, a label match — would be exactly the generic pointer patching this
contract forbids.

Three closed operations:

| Operation | Slot | Meaning |
|---|---|---|
| `set_process_root` | `root` | Establish the whole root. Two of these conflict, even byte-identical. |
| `insert_root_linear_step` | `root.before_terminal` | Insert one linear step before the root's indivisible terminal unit. |
| `append_root_terminal_leg` | `root.terminal.branch.legs` | Append one leg to the root's terminal Branch. |

"Terminal unit" is the tail the root cannot be split inside: a terminal control, a standalone
`return_documents`/`stop`, or the `target`+`stop` **pair** — never split, because a step
between a target and its stop is a step on no path the emitter can express.

Only the *final root* Branch is addressable. Nested branches have no stable semantic address.

### `SystemTopologyPatchV1`

Additive only: `add_object`, `add_relation`. No update, replace, remove, or lifecycle
operation exists — #144 ships a planner and this issue adds no mutation. The only
expressible relationships are the existing eight typed relations with their named role
fields; there is no generic edge operation, so a recipe cannot author a graph the topology
schema would not itself admit.

### `ComponentContributionV1`

Carries a **header** — key, type, materialization mode — plus a `materializer_slot` naming an
entry in a private, adapter-owned catalog. It carries no config, name, component id, profile
body, header, XML, dependency edge, or executable payload of any kind.

That split is the security design. The compatibility adapter already accepts SQL, hosts,
credentials and script bodies — those are its existing public inputs — so it builds the real
`IntegrationComponentSpec` objects itself and keeps them in a `MaterializationCatalog` that
is never serialized and never handed to an executor. The recipe sees only an opaque slot
name. The engine then resolves the slot and **verifies** the catalog entry's key/type/mode
equal the contributed header.

### `ConstraintRequirementV1`

Five closed checks: `component`, `process`, `topology_object`, `topology_relation`,
`capability`. There is no `passed`, `severity`, `waiver`, `exemption`, `safe`, expression,
callable, validator name, or caller-supplied remediation field. **A constraint can only add
an obligation; it can never discharge one.** `RequireCapability.required_state` admits only
positive states — a recipe that wanted the opposite would be asserting its own violation is
safe.

## 4. Forbidden shapes

`scan_forbidden_recipe_shape` runs on raw payloads **and** on `model_dump()` output, so
neither a raw dict nor a `model_construct`-built object can smuggle anything past it. It
runs *before* pydantic, so a credential is rejected as a security failure with a value-free
diagnostic rather than as a schema error whose message could echo the value back.

Rejected: free-form config bags; raw XML; headers; connection material (host, base URL,
port, driver, username, SQL, connection string, credential refs, certificates, environment
extensions); executable code (`code`, `script`, `script_body`, `language`, `module`,
`class`); generic paths, JSON pointers, indices, node/shape/CFG ids; generic graph edges and
component `depends_on`; and any caller-supplied provenance, capability verdict, validation
result or conflict priority.

Two rules are **stricter than direct ProcessIR authoring**:

* `custom_scripting` is rejected even though `CustomScriptingOpV1` is a real supported node.
  A recipe is code the registry vouches for, and vouching for arbitrary script text is
  precisely what "no LLM-generated Python" rules out.
* component `depends_on` is rejected because materialization order is builder-derived
  (`_topological_order`), not recipe-authored.

## 5. Registry, provenance and skew

`src/boomi_mcp/recipes/registry.py`. Deliberately **not** an extension of
`PatternRegistry`: that one scans a package at import and keys on a bare name with no
version selector or provenance. Here a recipe exists because a line in
`recipes/builtins/catalog.py` registers it. There is no runtime registration API, and a
test asserts its absence rather than trusting this sentence.

Two invariant classes, reported differently:

* **Build defects** — duplicate registration, two defaults for one id, an
  `async`/generator/lambda/partial/bound-method/closure executor, an undeclared output type, an
  unknown capability subject, an `(authority, required_state)` pair the authority can never
  report, a self-dependent prerequisite, and an adapter whose `adapter_target` is unregistered
  or not executable — all raise `ValueError` at construction. They are not a caller's problem.

  That list is prose and drifts. What does not is
  `test_every_reachable_registry_build_defect_is_exercised_by_a_test`, which runs the registry
  suite under a stdlib `sys.settrace` tracer and asserts that **every non-pragma
  `raise ValueError` in `registry.py` was actually executed**. It is a reachability proof, not
  a count — an earlier version was a count, justified by "asserting reachability needs a
  coverage tool this repo does not carry", which was simply false: a ~25-line tracer is that
  assertion. Adding an untested build defect fails it; so does removing the test that reaches
  one. The matcher counts both spellings, `raise ValueError(...)` and bare `raise ValueError`,
  because the bare form is exactly what a guard that says "every one" must not miss.
* **Caller-facing failures** — unknown id, missing version, gated capability — raise
  `RecipeError` with the taxonomy code and a value-free diagnostic.

### Version selection

Parallel versions coexist by `(recipe_id, recipe_version)`. At most one descriptor per id may
set `is_default`. A name-only call selects that code-declared default. **An exact request
never falls forward or backward** — a caller who asked for `1.2.0` and silently got `1.3.0`
would have no way to notice, which defeats pinning entirely.

Discovery sorts by id, then **parsed** SemVer, then entry kind. Parsed, not lexical: `1.10.0`
must sort after `1.9.0`. The sort makes discovery independent of registration order, which is
what lets `registry_revision` be a stable identity rather than an accident of import sequence.

### Provenance

Derived from the registered code, never accepted from a caller:

| Field | Source |
|---|---|
| `module` / `symbol` (executable / constraint-only) | the registered callable |
| `module` / `symbol` (adapter / advisory) | there is no callable — the catalog module, and the recipe id as the symbol |
| `implementation_sha256` (executable / constraint-only) | length-prefixed hash of module name + symbol + the function's source + **its defining module's source** + canonical input schema |
| `implementation_sha256` (adapter / advisory) | there is no function to hash — the catalog module's source + the recipe id, version, entry kind and adapter target (see "Entries with no executor") |
| `descriptor_sha256` | hash of the descriptor body **without itself** — a self-referential hash is unverifiable |
| `source_revision` | the image `BUILD_REVISION` file, else a `source-sha256:` digest of `RECIPE_LAYER_MODULES` — the recipe layer plus its **direct callers**, NOT everything a run executes (see the exclusions below) |
| `registry_revision` | hash over the canonically sorted descriptor list |

The **module** source is in the digest, not just the function's. Live QA caught why: both
sync recipes delegate their whole body to a shared `_contributions` helper, so hashing only
the registered function left every hash unmoved while a behaviour-changing edit turned
`build_from_archetype` from a working spec into a hard failure. A caller comparing hashes
would have been told `match` about a registry whose output had changed.

This is deliberately conservative — an unrelated edit elsewhere in the module moves the hash
too. That is the safe direction: a spurious `mismatch` prompts a look, a spurious `match`
ends the investigation. Per-symbol attribution survives, because each registration also
hashes its own source.

A helper in a *different* module remains outside the **per-entry** digest. Stated rather than
papered over: chasing the full import closure would move every recipe's hash on any change
anywhere, which is a hash of the package rather than of a recipe.

**The two hashes have deliberately different scopes**, and the second is what covers the
wider surface:

| Hash | Question it answers | Scope |
|---|---|---|
| `implementation_sha256` | *Which* recipe changed? | that entry's own defining module |
| `source_revision` | Did anything in the recipe layer **or its callers** change? | every module in `RECIPE_LAYER_MODULES` — the recipes package plus every module that invokes the engine |

`RECIPE_LAYER_MODULES` is governed by a rule narrowed until the prose, the list and the test
are the **same statement**. Three clauses, all mechanically decidable:

1. every module in the `recipes` package;
2. the two **contract** modules the layer is built out of — `models/recipe_contributions`
   (the contribution types) and `build_info` (the provenance derivation) — outside the package
   only because `models/` may not import `categories/` and `build_info` must stay stdlib-only;
3. every module that **invokes** the recipe engine.

Clause 2 is not a carve-out bolted on to cover two stragglers: it is clause 1's property
wearing a different directory. Both modules are this issue's own code, both change what a
recipe run produces, and both would live in `recipes/` if the import rules allowed it. "Invokes" is mechanical — a call to `run_recipes`,
`run_sync_preset_recipe` or `run_fanout_recipe` — so the test decides it by walking the
package for DIRECT calls — one level, not the transitive closure — rather than by
anyone's judgement. Today that is `recipes/*`,
`models/recipe_contributions`, `build_info`, `patterns/recipe_bridge`,
`patterns/composition.py` and the two migrated archetype modules. Round 2 of live QA found this claim documented
*before* it was true: the digest originally listed only the executor modules plus
`registry.py`, the same files the per-entry hashes already covered, so a behaviour-changing
edit to `engine.py` or `recipe_bridge.py` moved nothing at all while both migrated presets
went from a working spec to a hard failure. Round 3 caught the same class again, one layer out: the fix's own justification claimed
`source_revision` covered "the archetype modules" while the list did not name them, and a
renamed emitted component left the entire published snapshot byte-identical.

The narrowing is itself the fix for a recurring failure. **Four consecutive rounds of live QA
falsified a broader sentence than the list backed**, each time by finding a module the words
covered and the digest did not. The problem was never the missing module — it was writing a
claim no test could check. A rule an AST scan can decide cannot drift from its list.

The list is static (a scan would make the digest a property of the filesystem) and pinned in
both directions: against the package's real contents, and against every module that calls an
engine entry point directly. A fifth migrated surface fails the test until it is listed.

#### What `source_revision` does NOT cover, and why

Two sets are deliberately outside.

**The downstream canonical modules** — roughly 30 of them: `compiler/process_ir/*`,
`models/process_ir`, the component builders, the graph verifier. A recipe run executes them,
and editing one *can* change the emitted XML with nothing published moving. Round 4 of live
QA measured exactly that: a label change in the `sync_pipeline` legacy adapter altered the
emitted `userlabel` on three of the five emitted process shapes while the whole snapshot
stayed byte-identical.

**The reporting layer** — `categories/integration_authoring`, `integration_import`,
`meta_tools`. They read the registry to build a response and never invoke the engine
*directly*; transitively they reach it through `composition.py`, which is listed. One level is
the line the digest draws — the boundary between "this module runs recipes" and "this module
called something that does". An edit to a response builder can change published bytes without
changing any recipe's output.

The two package `__init__.py` shims are also unlisted: they are pure re-exports with no logic,
and every module they re-export is listed. The engine-invocation pin *does* scan `__init__.py`
files, so one that ever gained a call would be caught.

Both are excluded **on purpose**. Folding them in would make `source_revision` "a hash of the
package rather than of a recipe" — the same objection that keeps the *per-entry* digest
scoped to one module — and would move every recipe's revision on any compiler or
response-shape change. The downstream modules are separately versioned canonical authorities
with their own published capability manifests; `capability_revisions` tracks those, and a
deployed image's `$COMMIT_SHA` covers the whole tree regardless.

**In a source checkout, a change to an excluded module is not visible in
`source_revision`.** A real limit, stated rather than papered over. Two earlier revisions of
this section claimed coverage that did not exist, and each time live QA had to find it; an
honest boundary is worth more than an ever-widening digest.

### Entries with no executor

An adapter or advisory entry has no code of its own — it **is** a declaration. Its digest
therefore covers the declaration's actual source: the `builtins/catalog.py` module where it
is written, plus the adapter target it names. Live QA found the original form hashing only
`(recipe_id, recipe_version, entry_kind)`, which never moved for *any* edit — including one
to the catalog module itself — leaving four of eight published entries pinning nothing, in a
field the skew note tells callers to rely on. An adapter's runtime behaviour lives in
`patterns/recipe_bridge` and the migrated archetype modules, which no single entry can own;
that is `source_revision`'s job, and `RECIPE_LAYER_MODULES` names every one of them.

`inspect.getsource` failure raises at construction. An empty or defaulted hash would make
every skew comparison report `match`, which is worse than refusing to start.

Closures, lambdas, partials and bound methods are rejected as executors for the same reason:
their captured state is invisible to `inspect.getsource`, so their implementation hash would
be blind to the very thing that changes their behavior.

### Skew

`list_integration_archetypes(expected_recipe_registry=...)` compares a caller's snapshot
against the live registry and reports `not_requested | match | mismatch | unknown`, with
`missing_from_live`, `live_only`, `version_mismatches`, `implementation_mismatches`,
`registry_revision_mismatch` and `source_revision_mismatch` — all sorted.

**Equal versions are not proof of equal code.** Two registries can agree on
`api_to_api_sync@0.1.0` and run different bytes; supplying `implementation_sha256` is what
turns the comparison into evidence. A version-only match with a hash difference reports
`mismatch`.

`match` means the compared **fields** agree — never that two deployments behave identically;
the excluded sets above are exactly what it cannot speak for.

`unknown` never collapses into `match`, and that now covers the **partial comparison** case
as well: an expectation carrying neither revision *and* missing an `implementation_sha256`
on some entry is a version-only comparison. Every id and version can agree while the code
differs, so it reports `unknown` with a reason rather than `match`. A difference *found* is
still `mismatch`, however partial the comparison was — a finding is a finding.

A caller who asked for a comparison the server could not make must be told so.

### What the public descriptor omits

`RecipeDescriptorV1.public_payload()` redacts `capability_requirements` to a count. A
requirement's `subject` names a key inside a canonical authority, and for `process_emitter`
those keys are dark compiler internals that `tests/test_process_ir_compiler_surface.py`
forbids on any LLM-visible surface. The requirements are unchanged and still enforced at
preflight — this hides them from the caller, not from the gate.

## 6. Composition and conflicts

`src/boomi_mcp/recipes/composer.py`.

1. Invocations are topologically sorted on declared recipe dependencies, ties broken by
   `(recipe_id, parsed SemVer, invocation_id)`.
2. Each executor's returned contribution tuple keeps its own order.
3. Operations apply in a **fixed phase sequence**: roots → linear inserts → Branch legs →
   topology objects → topology relations → components → constraints.

The phase sequence is what makes "append a leg" well-defined regardless of which recipe ran
first: every root exists before any insert is attempted, and every object exists before any
relation resolves. Without it, two orderings of the same set could produce a
target-not-found in one and success in the other.

**Nothing is re-sorted after normalization.** Branch-leg order, contribution order and step
order are semantic.

### Declared merges

A merge is active only when **every** recipe writing the contested slot declares the rule:

`insert_root_linear_step` · `append_root_terminal_leg` · `dedupe_identical_constraint`

Three, not five. Earlier drafts also carried `append_distinct_topology_key` and
`append_distinct_component_key`, but distinct keys compose unconditionally and repeated keys
conflict unconditionally, so neither rule was ever consulted. A declared rule that gates
nothing is a false affordance — it tells an author they opted into a behaviour that was never
theirs to opt into. Removed rather than documented.

`dedupe_identical_constraint` collapses an equal requirement id with a byte-identical body.
An equal id with a *different* body is a naming collision, not a merge, and always conflicts.

### Conflicts

`RECIPE_PATCH_CONFLICT` for: two roots for one process (even byte-identical); a recipe root
replacing a direct-authored one; differing `profile_ref` for one topology; a repeated
topology object/relation key; a repeated component key; the same constraint id with
different bodies; and any otherwise-mergeable operation whose descriptors did not declare the
rule.

There is **no last-writer-wins and no priority setting**. `RecipeConflictPolicyV1.mode` has
exactly one value, which says that in the type rather than in a comment.

Every conflict diagnostic names **exactly two producers in normalized writer order**,
including when both recipe ids are equal — "this recipe conflicts with itself" is a real
situation, and collapsing the pair would hide which invocation was the second writer. A
direct-authored base appears as the literal `direct_authoring`.

`RECIPE_PATCH_TARGET_NOT_FOUND` when an insert/append has no root, when
`root.terminal.branch.legs` does not resolve to a terminal Branch, or when a typed relation
names an object key absent after all object additions.

## 7. The non-bypassable validation gate

`run_recipes` always walks the same funnel:

1. resolve the descriptor at an exact version
2. preflight declared capability requirements
3. preflight declared prerequisites — recipe **and** execution-context
4. forbidden-shape scan, the strict input model, then **`type(validated) is model`**, no
   instance extras, and every stored value checked against its declared annotation by an
   adapter **the engine built**
5. run **only** the registered callable — never caller-provided code
6. re-validate every returned value as a **declared** contribution type
7. run it a second time and byte-compare — nondeterminism is a hard failure
8. order and compose the closed operations
9. resolve component slots against the private catalog, **verifying headers**
10. `parse_process_ir_v1` → `compile_process_ir_v1(..., validation_policy=None)` →
   `emit_process` → `verify_process_graph`
11. `parse_system_topology_v1` → `plan_system_topology(..., "plan")`; blockers are fatal
12. evaluate every declared `ConstraintRequirement` against those artifacts

**`validation_policy` is hard-coded `None` at the compile call site, and `run_recipes` has no
parameter that could carry one.** That is what makes "a recipe cannot bypass semantic
validation" a structural fact rather than a convention: the legacy exemptions exist and are
reachable, but not from here.

Step 4's third clause is not redundant with the second. Two registration-time gates prove a
recipe input model is *declared* closed and *compiled* closed, both by reading the schema —
and the schema records that a validator runs at a position, never what it **returns**.
`model_validator(mode="after")` receives the constructed model and its return value becomes
the result, so a model that is frozen, `extra="forbid"`, and free of every banned node still
handed the caller's undeclared keys to the executor as a plain dict. Widening the node-type
ban is not available: `after` is used legitimately by a production input model to enforce a
cross-field rule. The engine therefore checks the **value that came back** rather than
classifying what ran, and by exact type — a subclass declaring `extra="allow"`, populated via
`model_construct`, satisfies `isinstance`.

**The root type is only the outermost layer of that boundary.** The same replacement one level
down — a nested model's `after`, or a `field_validator` — swaps the value at *its* position,
and pydantic does not re-check it, so the registered outer type survives while an ordinary
declared field holds the caller's stashed mapping. That reaches the surface an honest executor
actually reads: every production executor consumes its input by attribute
(`inp.source_connection_ref`).

So the engine walks the **values**, not the schema: each stored value is validated against its
declared annotation by a `TypeAdapter` **the engine built**, and any instance carrying
`__pydantic_extra__` is refused.

The walk recurses into *any* element of a container, not only the ones that are already models,
because the elements of a container are not the leaves. A first version tested
`isinstance(item, BaseModel)` there and so stopped at one level: `Tuple[Model, ...]` was covered
while `Dict[str, List[Model]]` and `Tuple[Tuple[Model, ...], ...]` were not, and a model used as
a mapping **key** was never examined at all. Pydantic dataclasses get their own branch — one is
neither a `BaseModel` nor a container, so a model reached through a dataclass field was walked
straight past (`is_dataclass` covers stdlib dataclasses too).

Each value is checked **strictly**, because the walk runs *after* validation: pydantic already
did any coercion, so a stored value should already BE its declared type. Lax mode re-permitted
that coercion and accepted `"5"` for an `int`, a list for a `Tuple[str, ...]`, and an ISO string
for a `datetime` — and worse, it *consumed* a generator stored in a sequence field, reporting no
mismatch and leaving the field exhausted. A check must not damage what it inspects.

Three more things the adapter alone does not settle, each found by review rather than by
construction:

* **Successful conversion is not proof.** Strict mode still admits a mapping where a model is
  declared, and then runs that model's validators — which hand the dict back. The adapter
  "succeeds", its result is discarded, and the stored value was never a model. So the runtime
  class is checked against what the annotation admits, not against whether conversion worked.
* **Element and field annotations are carried down.** A dict standing in for a `Leaf` inside
  `Tuple[Leaf, ...]` or `Dict[str, Leaf]` was visited but never judged, and a dataclass's fields
  were recursed with no annotation at all — leaving a mapping in a declared `str`, readable at
  `inp.holder.label`.
* **Anything the walk cannot enumerate SAFELY is refused** — a value that is not an
  `abc.Collection`, or that is its own iterator. `Iterable[str]` validates lazily:
  pydantic returns a `ValidatorIterator` without inspecting an element, so a replayable custom
  iterable yielding caller mappings passed untouched — and identically on both determinism runs.
  Consuming it to look would destroy it, so the only safe answer is to refuse it. The rule is
  "sized and re-iterable", not a list of concrete types: an earlier version named five container
  classes and refused everything else, including `range`, `dict_keys`/`values`/`items`,
  `array.array`, `memoryview` and any custom `abc.Sequence`.

The runtime-class check is scoped to **model and dataclass positions**, which is the only place
conversion can mislead. A wider version refused every `Any` field — `typing.Any` is a class from
Python 3.11, so `isinstance(value, Any)` raised — including `Dict[str, Any]`, the very shape §12
names as the truthful permissive declaration. It was also stricter than the `strict=True` adapter
one line above it, refusing an `int` literal default in a `float` field while the same field
passed whenever a caller supplied the value: two halves of one gate disagreeing about "declared
type", surfacing as an intermittent failure.

Cost after all of that is ~21 µs per invocation against ~2.4 µs for `model_validate`, with
annotation introspection memoised; the adapters and the unwrapping are both built once per
annotation. Cost is ~10 µs per invocation against ~2.5 µs for `model_validate` — 4x the
validation it guards, and ~7x the `model_dump` sweep it replaced.

That the adapters are the engine's own is the whole design, and it is the conclusion of five
retired checks rather than a preference:

| retired check | defeated by |
|---|---|
| `model_config["extra"]` | a mutable class attribute, read at check time, validator compiled at class creation |
| `ValidationError.from_exception_data` | a predicate over an object the author constructs — `type` and `loc` are both author-chosen |
| classify `function-wrap` by whether it reaches a model | undecidable from the schema; defeated four ways |
| `ser["function"].__module__` | a plain writable attribute; also matches third-party `pydantic_*` modules |
| ban any ser schema, read from `__pydantic_core_schema__` | that attribute is itself writable — assigning a closed twin's schema after class creation passes every registration gate while the compiled validator still accepts extras |

**The class is exhausted: every attribute of the author's class is writable, and registration
runs after class creation.** What an author cannot reach into is an adapter the engine
constructs from the declared annotation, applied to the value that actually arrived. A masking
serializer no longer matters, because nothing asks it anything.

Removing the serializer ban also removed false rejections it had introduced — `SecretStr` and
`pathlib.Path` register again, and `SecretStr` mattered: it is the type an author *should* reach
for on a sensitive input, so refusing it pushed them toward plain `str`.

**Known, measured, still open:** the wrap/plain *validator* ban refuses `AnyUrl`,
`IPv4Address`, `IPv4Network`, `re.Pattern` and `deque`, because each compiles to a
`function-plain`/`function-wrap` validator node. That over-fire predates the value-first check
and is not fixed by it. The value-first check appears to subsume what that ban guards — an
object of exactly the registered type, with no extras, whose every field matches its
annotation, is indistinguishable from an honestly validated one — but removing a gate that took
four review rounds to get right is a separate decision with its own evidence, not a side effect
of this one. `test_the_wrap_ban_still_over_fires_on_these` pins the current cost so it stays
visible.

This check moves one failure from **build time to first use**, and that is inherent rather than
an oversight: what a validator returns is a runtime fact and cannot be read off the schema at
registration. A bypass-shaped input model therefore registers successfully, is advertised by
`list_capabilities` and `get_schema_template`, and is refused only when someone invokes it. The
trade is accepted because registrations are code-owned and the production set is covered by
tests — but *when* the failure surfaces is different from every other gate here, which is worth
knowing before it is discovered.

Step 7 is why **executors receive their validated input and nothing else** — no context
object, no catalog. Handing an executor a context would reopen an I/O channel and make the
determinism proof weaker. `ExecutionContextPrerequisite` is a declaration the *engine* must
satisfy, not something the executor is given.

No recipe path calls `_build_plan`, `_apply_plan`, `_execute_component`, or any topology
apply. Full account-dependent component planning stays where it was; the recipe response is
a draft, not a claim that collision resolution ran.

## 8. Error taxonomy

Ten `RECIPE_*` codes, all owned by #145, all category `recipe`. The family is asserted as a
biconditional in `tests/test_error_taxonomy.py`.

The design plan specified nine. `RECIPE_REQUEST_INVALID` is the tenth, added during
implementation and recorded here rather than left as an undocumented extra: a duplicate
`invocation_id` is a defect in the **request envelope**, not in a recipe's input, and reusing
`RECIPE_INPUT_INVALID` for it told the caller their input carried "credentials, headers, SQL,
raw XML" — a misdiagnosis of exactly the kind this taxonomy exists to prevent.

`RecipeDiagnosticV1.code` is typed to this closed set (`RecipeErrorCode`), not to `str`. A
bare `str` made "all ten and nothing else" a convention resting on `recipe_diagnostic` being
the only constructor, which the public model never guaranteed.

| Code | Raised at |
|---|---|
| `RECIPE_NOT_FOUND` | registry lookup: the id is absent |
| `RECIPE_VERSION_UNAVAILABLE` | the id exists, the exact version does not; the diagnostic lists sorted available versions |
| `RECIPE_CAPABILITY_GATED` | descriptor preflight: an authority subject is absent, gated, guidance-only, unsupported, or weaker than required |
| `RECIPE_INPUT_INVALID` | the forbidden-shape scan, the strict input model, or a validator that returned something other than that model |
| `RECIPE_CONTRIBUTION_INVALID` | undeclared output type, forbidden field, invalid schema, duplicate operation id, header mismatch, missing catalog slot, or an executor exception |
| `RECIPE_PATCH_TARGET_NOT_FOUND` | a closed slot does not resolve after composition |
| `RECIPE_PATCH_CONFLICT` | two writers, one slot, no declared merge |
| `RECIPE_CONSTRAINT_FAILED` | a canonical validator or a declared requirement rejected the result; canonical codes ride along as sorted `cause_codes` |
| `RECIPE_OUTPUT_NONDETERMINISTIC` | two runs over identical input produced different canonical bytes |
| `RECIPE_REQUEST_INVALID` | the request envelope, not a recipe input: a duplicate `invocation_id` |

The recipe layer blames the **recipe layer**. A canonical rejection never becomes a
`PROCESS_IR_*` or `TOPOLOGY_*` code attributed to #145 — it becomes
`RECIPE_CONSTRAINT_FAILED` carrying the canonical code as a value-free cause. That keeps the
canonical taxonomies authoritative about their own domains while still telling the caller
which layer they must fix.

Every message and remediation is a **static table lookup**. Nothing is interpolated from an
exception, an input, a component name, a label, or a reference.

## 9. Migrated surfaces

| Surface | Recipe | Adapter |
|---|---|---|
| `compose_archetypes` | `boomi.compose.db_rest_fanout@1.0.0` | `boomi.adapter.compose_archetypes@1.0.0` |
| `api_to_api_sync` | `boomi.archetype.api_to_api_sync@0.1.0` | `boomi.adapter.api_to_api_sync@1.0.0` |
| `api_to_database_sync` | `boomi.archetype.api_to_database_sync@0.1.0` | `boomi.adapter.api_to_database_sync@1.0.0` |

The other four archetypes and the 25 primitives are **not** migrated and are reported
honestly as such — an unmigrated archetype returns no `recipe_provenance` rather than a
plausible-looking reference to code it does not use.

### The cache-lineage asymmetry

The legacy `flow_sequence` adapter mints an **occurrence-scoped alias per reference**, so the
staging `cache_put` and each consuming `cache_get` name *different* symbols. The strict
lineage validator then cannot see the writer from the reader, and the dialect needs its
registered `STANDALONE_CACHE_READ` exemption to compile at all.

The native fan-out recipe uses **one stable `$ref:handoff_document_cache`** across the put and
every get, so the same strict validator sees the sequential writer and passes with
`validation_policy=None`.

That asymmetry is *measured*, not assumed. `tests/patterns/test_recipe_preset_parity.py`
asserts both halves: the recipe arm compiles strictly, **and** the legacy arm on the same
composition still raises `PROCESS_IR_SEMANTIC_LINEAGE_CACHE_WRITER_MISSING` without its
policy. If the second half ever stops failing, the test fails — so the claim cannot rot into
a tautology.

## 10. Parity

Four oracles, strongest first. None subsumes another:

* **L1 — full `IntegrationSpec` dump** against fixtures captured at baseline commit
  `060dabad64e028d83d192e5820d8f37df64d54d3` in a git worktree, before any of this issue's
  code existed. A fixture captured from the migrated tree would be a snapshot of the code it
  is supposed to check.
* **L2 — ComponentPlan order** from `_build_plan`. L1 cannot see this: `_topological_order`
  is a *sorted* topological order, so declaration order alone does not determine
  materialization order.
* **L3 — process XML differential** between the recipe arm and the legacy adapter arm under
  one shared resolver. Both arms terminate in the same `emit_process`, so any difference can
  only come from the IR — which is exactly what the migration changes. Committed goldens
  follow as a uniform-drift detector; a golden alone would be satisfied by both arms drifting
  together.
* **L4 — the exemption asymmetry** above.

Cases covered: compose stream / mixed-cache / all-cache / 25-target stream / 24-target cache,
plus both presets across every published example.

## 11. Rollout

Incremental, with a per-recipe parity gate and **no early deprecation**. `emit_fragment` and
the `PatternRegistry` remain exactly as they were, documented as retained compatibility
surfaces. Migrating a further archetype means: build its catalog, project a safe input,
register the recipe, add it to `RECIPE_LAYER_MODULES`, capture baseline fixtures, and add its
parity case — the same six steps the three migrated surfaces took.

## 12. Deviations from the design plan

Recorded here because a deviation nobody wrote down is indistinguishable from a constraint
that was dropped. Each states what changed and why.

* **`append_root_branch_leg` → `append_root_terminal_leg`.** This is a `Literal` in a public
  contribution schema, so the rename changes the JSON contract, not just a class name. The
  operation appends a leg to the root's TERMINAL unit, which is a Branch today; naming it
  after the branch published a compiler-internal shape word on a public surface, which
  `test_no_compiler_internal_in_any_schema_template_payload` rejects. The new name describes
  the position the operation targets, which is the part the contract actually fixes.
* **Executors receive only their frozen validated input**, not an execution-context object.
  Context prerequisites became a declaration the ENGINE must satisfy. Removing the channel
  removes an I/O path and strengthens the double-execution determinism proof.
* **Two of the five merge rules were removed.** `append_distinct_topology_key` and
  `append_distinct_component_key` were never consulted: distinct keys compose
  unconditionally and repeated keys conflict unconditionally, so neither rule had a decision
  to make.
* **`contribution_kind` was added** as a shared discriminator, with
  `RECIPE_CONTRIBUTION_KINDS` derived from the union rather than listed beside it.
* **`RecipeComponentType` is a pinned copy** of the builder's type set (`models/` may not
  import `categories/`), deliberately excluding `trading_partner`.
* **SemVer is implemented in-tree** rather than adding a dependency. The published regex is
  written for a dialect where `\d` is ASCII and `$` anchors the end of input; the in-tree
  copy pins both (`re.ASCII`, `\Z`) because Python's defaults differ on each.
* **`source_revision` covers the recipe layer and its direct callers only.** ~30 downstream
  compiler modules and the reporting layer are excluded, and the bound is stated at
  `RECIPE_LAYER_MODULES`.

### Deliberately not implemented

* **`unknown/registry_snapshot_unavailable`.** The plan's fallback describes how a *reader*
  interprets a response from an older server that predates the snapshot field. No such
  reader exists in this repo — a new server cannot make an old one answer — so implementing
  it here would mean writing an unused client path. The two analogues that ARE reachable are
  implemented: `RecipeRegistrySkewV1` returns `unknown` with a reason when the comparison is
  partial, and `list_capabilities` reports `recipe_registry: {"status": "unavailable"}` when
  the local snapshot cannot be built.
* **Centralizing the advertised server version.** The plan asked `meta_tools` to use "the
  centralized server version". The string `"1.3"` occurs exactly once, and the only
  package-level constant is `__version__ = "0.1.0"` — a different fact. There is nothing
  duplicated to centralize, and wiring the package version in would misreport the server
  version.
* **Re-checking field CONSTRAINTS after validation.** The step-4 check compares a
  stored value against its declared *type*, not against its constraints, so a
  `field_validator(mode="after")` that returns another `str` for a `Literal["a"]` or
  pattern-constrained field is not caught. Three measurements say leave it:

  **Containment — the check would be redundant where it matters.** Two later gates already
  cover it, and they cover different things:

  *Values that flow* are re-validated by the contribution models, independently of the input
  model. A lie about a component key or a ref is rejected at `_run_executor` with
  `RECIPE_CONTRIBUTION_INVALID` — measured for a padded `process_key`, one carrying a control
  character, and an emptied `source_connection_ref`.

  *Values that do not flow* are outside that gate by construction: re-validating what a recipe
  returned cannot see what it never returned. A `component_slots` validator returning `()` — a
  `min_length=1` lie — passes the sub-tree sweep (an empty tuple is the right type) and produces
  a run with every `component_contribution` missing. What refuses it is the declared
  `ConstraintRequirement`s at funnel step 12: `RECIPE_CONSTRAINT_FAILED`. Naming only the first
  mechanism here overstated what re-validation does; both are needed to make the claim true.

  The only survivor found across both is `version` violating `^1$`, and it survives precisely
  because nothing downstream reads it: inert, not corrupt. So the re-check would duplicate an
  existing gate for the values that flow, a different gate for the ones that do not, and cover
  only values with no consequence.

  **Capability — the lie reaches nothing a truthful declaration cannot.** The sub-tree sweep
  confines any replacement to the declared *runtime type*, and every runtime type has a
  truthful permissive declaration that passes both registration gates. Across eight constraint
  families (`Literal` on `str` and on `int`, pattern, `MinLen`, `conlist` min and max, `Set`,
  `Tuple[str, ...]`) there was no payload whose only carrier was a violated constraint.

  **Cost — it would cost false rejections and buy blindness.** Per-field re-validation misfires
  on `Json[...]`: the stored value is the parsed object, while the annotation re-validates
  against the JSON string, so an honest field is refused. (`TypeAdapter(field.annotation)` alone
  does *not* misfire, because `Json` lives in `field.metadata` and is stripped — but every form
  that preserves the metadata, including the obvious `Annotated[ann, *metadata]`, rejects with
  `json_type`.) And on nested-model fields it would not work anyway: `revalidate_instances`
  defaults to `'never'`, so re-validating a stored model instance **accepts it unexamined** — a
  nested instance whose fields have been corrupted passes. Both production input models carry
  nested models (`SyncRecipeInputV1.component_slots`, `ComposeDbRestFanoutInputV1.targets`), so
  that blind spot is exactly where the check would be expected to earn its keep.

  What remains is a **transparency** gap, not a containment one: such a model lies about the
  schema `get_schema_template` publishes. No machinery is misled — nothing in `src/` validates a
  value against a published schema, and the skew comparison is schema-to-schema, so a lying
  model's hash is stable and compares correctly. The misled party is a human reader, who has the
  validator in front of them in the same class.
* **Closing the author-cooperation side channels.** Three measured channels move caller data
  past every check, and all three are left open deliberately:
  a `mode="before"` validator stashing the raw mapping in a module global; an `after`
  validator writing it to `__pydantic_extra__`; the same via `object.__setattr__` on
  `__dict__`. In each case the returned object is exactly the registered type, `model_dump()`
  and the declared fields are clean, and the data is readable only by an executor that goes
  looking for it under a name the model author chose. **Both halves are registered code,
  authored by the same person and hashed into `implementation_sha256`** — this is a covert
  channel between two cooperating pieces of trusted code, not a caller reaching the executor.
  It is also not closable in general: the module-global stash leaves no trace on the object
  at all, so any amount of object inspection would still miss it, and adding checks that
  close two of three would buy the appearance of a boundary rather than a boundary. What the
  guard does promise, and keeps, is narrower and checkable: **the executor receives exactly
  the declared model type**, so caller-controlled undeclared keys never arrive on a surface
  an honest executor reads.

## 13. Related documents

* `docs/architecture/ADR-001` — authority model, §6 (no duplicate authority), §7 (error
  families), §11 (secret boundary)
* `docs/architecture/M12_COMPATIBILITY_INVENTORY.md` — the full surface classification
* `docs/architecture/SYSTEM_TOPOLOGY_V1.md` — the topology contract (#144)
* `docs/INTEGRATION_AUTHORING_ROADMAP.md` — migration policy
