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
* **Element annotations come from the union arm whose CONTAINER matches the value.** Taking the
  first arm that had parameters at all broke both ways: `Union[List[Leaf], List[Other]]` holding
  `[Other()]` was refused because the `Leaf` arm's parameters were imposed on another arm's
  value, and `Union[Tuple[Any, ...], Dict[str, Leaf]]` holding a mapping was *accepted* because
  the tuple arm's `(Any, Ellipsis)` was unreadable as key/value, so both mapping annotations were
  dropped and a dict standing in for `Leaf` went unjudged. It abstains when no arm matches and
  when no arm matches. When SEVERAL match it judges them **disjunctively** — an element is
  acceptable if any matching arm admits it. Abstaining there was a second, worse bug: every
  same-origin union (`Union[List[Leaf], List[Other]]`) matches more than one arm, so abstention
  disabled the element check for all of them, and the false-rejection case above began passing
  because *nothing* was consulted. **ONE arm must cover the whole container.** Judging elements independently let a
  heterogeneous `[A(), B()]` satisfy `Union[List[A], List[B]]` — element 0 chose the `A` arm and
  element 1 the `B` arm, so the stored list matched neither declared arm, while the adapter
  accepted it via `List[A]` (because `A` converts a `B`) and discarded the conversion. An arm
  that is shorter than the value fails rather than abstaining at the indices it does not reach.
* **"No arm matched" is not "this annotation says nothing".** A custom generic whose core schema
  is a list stores a plain `list`, which is never an instance of that generic, so no arm matched
  by container and its elements were walked unjudged. When arms exist but none matches, every
  parametrised arm is applied instead — safe precisely because one arm must cover the whole
  container, so an arm that does not fit simply fails.
* **Abstention is only safe where something else judges the
  value** — for membership the last-chance loop does; for element parameters nothing does. For the
  same reason a `None` element annotation (a fixed-length arm running off the end) is dropped from
  the candidate list rather than tried: `_assert_declared_shape(value, None)` succeeds
  unconditionally, so one `None` accepted the element unjudged. Failed trials roll back by
  journal — a candidate that walks part of the tree before failing would otherwise leave nodes
  marked in the shared cycle guard and let a later candidate short-circuit past them. `Union[Tuple[Any, ...], Tuple[Leaf, ...]]` holding that same dict still
  passes, correctly: the value genuinely satisfies the `Any` arm.
* **An annotation form the walk does not recognise is UNWRAPPED, not waved through.** A 3.12
  `type X = ...` alias and a `NewType` were each returned as their own single option, so no
  element parameters were found and the contents were walked unjudged — while the adapter
  resolved the alias perfectly well. "I do not recognise this" and "there is nothing to check
  here" must not be the same value; that is the same defect as a `None` element annotation, and
  it is the third place it appeared.

  Unwrapping has to preserve the alias's own namespace: the raw `__value__` of `type X =
  list['Leaf']` carries a `ForwardRef` that reaches an adapter with no namespace and refuses
  every invocation, so it is resolved against the module the alias was written in. A
  *subscripted* alias (`A[Leaf]` where `type A[T] = list[T]`) is not a `TypeAliasType` and has no
  `__value__` at all — its origin is the alias, and its parameters have to be substituted before
  its elements are judgeable. `__supertype__` is unwrapped only for a real `NewType`; reading it
  unguarded refused honest instances of any class that happens to carry the attribute and is a
  usable field type.
**The annotation forms the walk RECOGNISES are enumerated, and that list is the boundary.**
Outside it, coverage degrades to what the adapter alone provides — and the adapter *converts*, so
that is strictly weaker. Three paths refuse instead of degrading: an iterable the walk cannot
enumerate safely, a dataclass or `TypedDict` whose hints will not resolve, and an alias that names
something undefined.

**This boundary has two halves with opposite properties, and one sentence used to cover both.**

*The annotation-form half is a coverage boundary and is closable in principle.* A gap here puts
caller data in a **declared** field an ordinary executor reads, through an **honest declaration plus
caller-controlled input** — no construct in the model whose only purpose is the bypass. `Iterable[str]`
validating lazily, `Union[List[A], List[B]]` holding `[A(), B()]`, a `TypedDict` judged in neither
dimension, a PEP 695 alias walked unjudged, `Dict[str, Any]`. These are **bugs**, they are what the
value-first walk exists for, and they are fixable.

*The read-interception half is not closable, and saying otherwise was the error that drove six
review rounds.* §7 already concluded that "the class of reading an attribute of the author's class is
exhausted: every one of them is writable, and registration runs after class creation." Every guard
added afterwards kept reading those same artifacts through less-forgeable **accessors** —
`type.__dict__["__dict__"].__get__` and friends — which makes the *read* unforgeable and does nothing
about the *content* being author-written. A registration-time gate loses to one assignment after a
class body. Findings of this shape are **accepted residue**, not bugs.

**The criterion, decidable from a repro script alone:** does the reproduction require author class
machinery — a metaclass, a descriptor or plain class attribute under a field name, a redefined
`__dict__` / `__pydantic_extra__`, a post-hoc rewrite of `__pydantic_fields__[...].annotation`, an
`__instancecheck__` or `__hash__` that writes? If yes it is residue. If the payload arrives through
an honest declaration and caller data alone, it is a bug. **Refusing ordinary supported Python is
always a bug, and ranks above both** — this layer shipped five such refusals (`OrderedDict`, a
`NamedTuple` field, `Counter`, `defaultdict`, every `__slots__` dataclass, `cached_property`), three
of them introduced by hardening against residue and two hitting this repository's own types.
The exact-type container rule that caused the last of them was relaxed back to an `isinstance` test:
a subclass overriding its own enumeration is residue, and refusing supported Python to close residue
is the wrong trade in both directions.

**Measured per item, because "all of them are fixed now" was published here once and was false.**
`OrderedDict`, a `NamedTuple` field, every `__slots__` dataclass and `cached_property` are accepted
and judged. `Counter` needed a second fix and got one: `_mapping_arms` read its single parameter as
the VALUE type, which is right for a dict-shaped generic and wrong for `Counter[K]`, where the
parameter is the KEY and the value is always `int` — so `counts: typing.Counter[str]` was refused on
every honest invocation with ordinary caller input. The docstring that reads "if some generic ever
parametrises its KEY instead" named a hypothetical that turned out to be in the standard library.

**`defaultdict` is still refused, and by a different gate**: `DefaultDict[str, str]` compiles to a
`function-plain`/`function-wrap` validator node, so the wrap/plain validator ban rejects it at
REGISTRATION, exactly as it rejects `Sequence[str]`, `Deque[str]`, `AnyUrl`, `re.Pattern`, `Fraction` and all
SIX `ipaddress` types (`IPv4Address`/`Network`/`Interface` and their v6 counterparts) — twelve
annotations, and the list is the family rather than a sample of it. That ban is
recorded below as known, measured and still open; it is named here too because a reader checking
"is `defaultdict` supported?" should not have to find the answer two subsections away.

Why residue is the right disposition rather than a defeat: **every one of those attacks is dominated
by a channel §12 already accepts.** A registered model that stashes the caller's raw mapping in a
module-level dict, never touching the instance at all, reaches the executor identically — measured
side by side against the metaclass attack. An adversary who can write a metaclass `__hash__` can
write `_STASH = {}`. Registered input models and executors are statically catalogued, reviewed server
code and are part of the trusted computing base; MCP callers control request data and cannot register
or replace executable Python (§5). Protecting only against malicious recipe *classes* while trusting
the rest of the Python in the same process is not a coherent boundary.

So: `_assert_declared_shape` is defence in depth against malformed caller data, accidental validator
substitution, and type confusion in trusted recipes. **It is not a sandbox, and not an
information-flow guarantee against a malicious or cooperating registered model.** Registered Python
may retain or communicate data through module globals, private or computed attributes, class
mutation, descriptors, metaclasses, imports or I/O. Provenance hashes identify code and detect skew;
they do not confine it. "Executors receive only their validated input" means the engine passes no
context or catalog argument — nothing more.

The one case where the module-global channel and a declared-field payload are **not** equivalent is a
hostile-or-buggy input model consumed by *someone else's* honest executor: the stash needs the
executor to cooperate, a payload in a declared field does not. That pairing does not exist today —
every registration pairs a model and executor from the same package — and `_check_input_owner_shared`
now enforces it as a build defect rather than leaving it an assumption.

**Any future runtime, marketplace or third-party executable-registration path changes this threat
model.** It would need a data-only design or an isolated execution boundary introduced *before* such
code is admitted; this layer cannot host untrusted code whatever the input handoff looks like,
because §12's channel is open by construction. Note also that `run_recipes` accepts a `registry=`
argument and `RecipeRegistry` is publicly exported: no MCP tool exposes either, but in-process Python
can supply an alternate registry. That is an injection/test seam, not a supported extension point,
and it presupposes arbitrary code execution.

`Annotated`; both union spellings (`typing.Union` and `X | Y`); `TypeAliasType`, subscripted and
not; `NewType`; `TypedDict` from either `typing` or `typing_extensions`; parametrised containers
and mappings; dataclasses, pydantic and stdlib. Anything else abstains, and abstention is safe
here **only because the adapter runs first** — that sentence is the whole safety argument for this
layer.

The list is a boundary rather than a completeness claim, because the input language keeps
growing: every defect found in this layer came from a declaration form, and PEP 604 unions, PEP
695 aliases, the second `TypedDict` spelling and subscripted generic aliases were each one such
form arriving. A form a later Python adds is therefore a *documented* gap a reader can check
against, not a silent one to be rediscovered — the same move §12 makes for the author-cooperation
channels.

* **A `TypedDict` field is read from its per-key hints** — via `typing_extensions.is_typeddict`,
  because `typing.is_typeddict` returns False for a `typing_extensions.TypedDict` subclass and
  both spellings are usable field types. A `TypedDict` has no `get_origin`, so no
  parametrised arm matched it, and it cannot be instance-checked, so the class check abstains —
  leaving it unjudged in *both* dimensions for a field type that registers perfectly well.
* **A dataclass whose hints cannot be resolved is refused, not walked blind.** Falling back to
  `{name: None}` walked every field with no annotation, and it is reachable without exotica:
  `from __future__ import annotations` plus a `TYPE_CHECKING`-only import. A pydantic dataclass
  cannot get there; a stdlib one can, and the walk handles both.
* **Type arguments are read POSITIONALLY only for a matched `tuple`.** Everywhere else an arm is
  usable only when its arguments are *uniform* — one argument, or the `Tuple[X, ...]` spelling —
  and an arm nobody can read is refused rather than applied. A list-backed generic whose schema
  uses its SECOND argument had index 0 judged against the FIRST, which accepted a plaintext string
  where a `SecretStr` was declared and rejected the correctly wrapped value, both at once. The
  `matched` qualifier is load-bearing in its own right: reading an *unmatched* `Tuple[str,
  SecretStr]` arm positionally would index into a list that arm does not describe. A mapping
  recovers more — two arguments as key and value, one as the value type — but three has no
  reading, and yielding no pairs there once meant every entry was walked unjudged. This rule has
  two consumers, the sequence walk and the mapping walk, and the first version of it was applied
  at only one; a generic that *subclasses* its backing container matches by origin and so took the
  unguarded path. **A rule with N consumers is not fixed until it is fixed at all N.**
* **Field annotations are carried down.** A dict standing in for a `Leaf` inside
  `Tuple[Leaf, ...]` or `Dict[str, Leaf]` was visited but never judged, and a dataclass's fields
  were recursed with no annotation at all — leaving a mapping in a declared `str`, readable at
  `inp.holder.label`.
* **Anything not KNOWN to be replayable is refused.** Walkability is an enumerated list of
  types — the builtin containers, `range`, the dict views, `array.array`, `memoryview` — not an
  inference from an interface. `abc.Collection` guarantees `__len__`, `__contains__` and
  `__iter__`, and none of those promises a *fresh* iterator: a collection handing out one shared
  internal iterator passes `iter(v) is not v` and is drained by the walk before the executor sees
  it, and a finite `__len__` does not stop `__iter__` yielding forever. `Iterable[str]` validates lazily:
  pydantic returns a `ValidatorIterator` without inspecting an element, so a replayable custom
  iterable yielding caller mappings passed untouched — and identically on both determinism runs.
  Consuming it to look would destroy it, so the only safe answer is to refuse it. The rule is
  "sized and re-iterable", not a list of concrete types: an earlier version named five container
  classes and refused everything else, including `range`, `dict_keys`/`values`/`items`,
  `array.array`, `memoryview` and any custom `abc.Sequence`.

  **Membership is by `isinstance`, and the enumeration-policing guards are gone.** They were added
  to close subclass divergence, widened four times across three rounds, and cost three refusals of
  ordinary Python — `OrderedDict`, a `NamedTuple` field and `Counter`. (The `__slots__` dataclass and
  `cached_property` refusals belong to two OTHER guards, `_is_slot_descriptor` and
  `_unexpected_storage`, which are still present and still credited with them; a container guard
  could never have seen a non-container.) Every attack they stopped needs author class machinery and
  is therefore residue.
  A guard that defends only residue asserts a property this layer does not claim, so it was deleted
  rather than left untested — leaving it would invite the next round to harden it again.

  **Other guards defend only residue and were kept, which needs its own reason.**
  `_assert_no_attribute_hooks`, `_assert_fields_are_not_intercepted` and the read-back check all
  stop author class machinery too. The distinction is FALSE-REJECTION COST, measured rather than
  assumed: the container-enumeration guards refused three ordinary Python types and had to be
  widened four times to keep doing so, while these three refuse nothing in a 407-class sweep of this
  package. A residue guard that costs nothing is defence in depth; one that refuses supported Python
  to keep its coverage is a bad trade, and that is the line — not the residue classification alone. What
  follows records why the guard existed, so the reasoning is not rediscovered from scratch.

  **Was: membership by EXACT TYPE, with the instance carrying no state of its own.** Testing it
  with `isinstance` re-opened the hole the enumeration exists to close, because a subclass may
  replace the very method being vouched for — a one-shot `__iter__` drained by the first union arm
  leaves the second walking nothing and "covering" the container; an `__iter__` that disagrees with
  its own storage lets the walk judge clean elements while a subscripting reader gets the payload.
  Snapshotting answers neither: a snapshot is *built by iterating*, so it inherits the same lie.

  What replaced `isinstance` first was a scan for redefined accessor names, and **that approach was
  abandoned on evidence.** It was widened four times across three review rounds — one-shot
  `__iter__`, an under-reporting `__len__`, an instance-shadowed `items`, a class-level
  `__getattribute__` supplying `values`/`get` under no name at all — and a census then found it
  guarding **7 of the 19** reads through which a `dict` or `list` subclass can hand back author
  data: `copy`, `popitem`, `setdefault`, `pop`, `update`, `fromkeys`, `__or__`, `__eq__`,
  `__reversed__` and `__contains__` were all unguarded, and every method a future Python adds would
  join them. An enumeration of mechanisms cannot be completed, so the rule stops enumerating.
  `type(value)` is unforgeable — a metaclass cannot change what `type()` returns — and an exact
  known type has no author code on it to run, which makes every accessor irrelevant at once.

  The cost was measured before it was accepted. Across the whole suite the walk sees only exact
  `tuple`, `dict`, `list`, `range`, `OrderedDict`, `dict_keys`, `dict_items` and `memoryview`;
  every subclass observed is a test fixture, and all five container-typed fields in
  `PRODUCTION_REGISTRATIONS` are `Tuple[Model, ...]` holding an exact `tuple`. **What it refuses is
  a real capability loss, stated rather than hidden:** a `NamedTuple` field, `Counter`, and
  `defaultdict` are all fail-closed now. `defaultdict` is a *correct* refusal — its `__missing__`
  manufactures a value from an author-supplied factory on subscript of an absent key — but the
  `NamedTuple` is simply a supported Python shape this layer no longer accepts, because
  distinguishing a safe `tuple` subclass from a hostile one is the enumeration that just failed.

  `OrderedDict` is an exact member, and the second condition is what makes that safe. Of the
  thirteen exempted types it is the only one whose instances accept attributes at all, and the
  exemption added for it originally short-circuited the instance check: a shadowed `items` on an
  `OrderedDict` reached the executor while the identical shadow on a plain `dict` subclass was
  refused. Exact membership and carrying no state are two conditions and both are required.

  **The same reasoning had to be applied to the MODEL walk, which no container rule touches.** It
  read every field with `getattr`. A registered ROOT input model defining `__getattribute__` needs
  no container, no validator and no unusual annotation — it is author code, so it returns the
  honest value while the walk looks and the caller's mapping to every read afterwards. So field
  values and `__pydantic_extra__` are read out of instance storage, and a class carrying
  `__getattribute__` or `__getattr__` is refused. Those two hooks are closed by the language, but
  they are not the only author code an ordinary read can run: a **descriptor installed under a
  declared field's own name** intercepts that field while wearing neither hook's name, and is
  refused for the same reason. `_check_input_schema_closed` sees none of this — it reads the
  emitted JSON schema, and all three are statements about the class body.

  **Three further reads had to stop asking the class, and the pattern in them is worth stating.**
  `type(value).model_fields` is ordinary attribute access, so a *metaclass* `__getattribute__`
  answers it — and a metaclass lives on `type(type(value))`, which no scan of the value's own MRO
  ever reaches; returning the real mapping minus one entry meant that field was never visited.
  `object.__getattribute__(value, "__pydantic_extra__")` bypasses a class `__getattribute__` but
  still runs *descriptor* lookup, so a `__pydantic_extra__` property — with a setter, which is what
  makes it constructible — answered the undeclared-key check with `None` while diverting the real
  extras elsewhere. And both MRO scans originally **stopped** at `BaseModel`: for
  `class Evil(RecipeInputBase, Mixin)` C3 linearises as `[Evil, RecipeInputBase, BaseModel, Mixin,
  object]`, so stopping put the mixin out of reach while Python's own lookup still found the hook
  there. **Skipping a trusted base is not the same as stopping at one**, a redefinition of the
  storage the gate reads is refused outright, and the declared field list is cross-checked against
  what the instance actually stores — because a class body can be written to as well as read from.

  **Three rounds of naming interception mechanisms ended the way the container rounds did.** A
  PLAIN class attribute under a field name — with the instance's own entry popped — is neither a
  hook, nor a descriptor, nor storage, so every enumerated check passed it while Python's ordinary
  lookup returned the caller's mapping. What closes that class is not a fourth entry on the list:
  the walk now **compares an ordinary read against what it judged**, which tests the property the
  layer actually needs rather than the ever-growing list of ways it can fail to hold. Two further
  reads were forgeable the same way and are read unforgeably now — `is_dataclass` is class-level
  attribute access, so a metaclass could deny it and skip the entire dataclass branch *including
  that branch's own guards*, and `__dataclass_fields__` could be truncated because only the model
  branch had a cross-check.

  **And the registration gates now leave a record, because everything they read stays writable.**
  Rewriting `__pydantic_fields__[name].annotation` to `Any` leaves the field *name* set identical,
  so a cross-check on names sees nothing, and the walk then judges the caller's mapping against
  `Any`. Re-running the schema gate does not catch it either: pydantic caches the compiled schema,
  so the forgery is invisible there until something forces a rebuild, while the walk reads
  `FieldInfo.annotation` directly and sees it at once. The registry therefore records each
  registered class's annotations at the moment its gates pass, and the engine asserts they are
  unchanged. **Coverage boundary:** that record covers the *registered* input graph, which is the
  only graph that reaches the engine.

  **Not walked, and stated rather than discovered:** private attributes and computed fields. They
  are attributes on an object the gate has certified, so an executor must not read them as data.

  The `__slots__` allowance is the mirror-image lesson. Treating every descriptor under a field
  name as interception refused fifteen slotted dataclasses on honest input — eight of them this
  repository's own process-IR types — on *every* invocation, because `__slots__` puts a
  `member_descriptor` under each field. `__objclass__` separates the compiler's descriptors from an
  author's, and it is load-bearing rather than decorative: a `member_descriptor` can be lifted off
  one class and installed on another, where it reads a slot of a class the value has nothing to do
  with.

  **Stated boundary:** the size bound asks for the value's `len()` through its base, so an
  overriding `__len__` cannot shrink it; an *unsized* value abstains and is bounded only by the
  walk itself.

The runtime-class check applies to **every class-typed option**, with exemptions **enumerated
rather than inferred** — because both attempts to infer them failed. A blanket rule tripped over
`typing.Any` being a class from Python 3.11, so `isinstance(value, Any)` raised and every `Any`
field was refused, `Dict[str, Any]` included. Scoping to *model* positions fixed that and reopened
the bypass: in `Union[Leaf, str]` the `str` option made the check return, discarding the `Leaf`
already collected, and a dict carrying the caller's key reached a declared field again. It also
missed conversion-capable wrappers — `TypeAdapter(SecretStr).validate_python("plain", strict=True)`
*succeeds*, building a new `SecretStr` whose result is discarded, so a raw string survived in a
`SecretStr` field with the redaction gone.

The exemptions are therefore a list: `Any` and `object`, and the five numeric widenings. A value
matching no class gets one last check against the annotation's non-class options, which is what
keeps `Union[Leaf, Dict[str, str]]` honest.

Strict mode **converts**; it does not merely check. That is the sentence the whole design turns
on.

**Both union spellings are flattened.** `get_origin(Leaf | str)` is `types.UnionType`, not
`typing.Union`, so an earlier version left the PEP 604 spelling as one opaque option — the class
list came out empty and the check was simply off under the ordinary modern syntax. The two are
`==` *and* hash-equal, so the memo returned whichever was computed first for both: a model written
`Optional[SecretStr]` lost its check whenever another model in the process used `SecretStr | None`
earlier. Import-order-dependent security behaviour, fixed by making both flatten to the same
result rather than by keying the cache differently.

A class that cannot be instance-checked yields no opinion **for that option**, which is then
judged by the adapter. Abstaining for the whole annotation cancelled checks that had already
failed: in `Union[SecretStr, SomeTypedDict]` the uncheckable `TypedDict` suppressed the failed
`SecretStr` check and a raw secret reached the executor. The adapter can still say a `str` is not
a `TypedDict`. A bounded
`TypedDict` is a class, passes both registration gates, and is correctly stored as a `dict` — but
`isinstance(value, SomeTypedDict)` raises `TypeError`, which failed *every* invocation of an
otherwise valid recipe. That is the second such class after `typing.Any`. The stdlib list is closed at three usable
members — `Any`, and both `TypedDict` flavours — but the general case is not: `__instancecheck__`
is arbitrary user code and a class carrying `__get_pydantic_core_schema__` is a legal field type,
so a metaclass raising `ValueError` is reachable. The guard therefore catches **any** exception
from the attempt rather than enumerating the forms, and it abstains for the whole annotation rather than
dropping the unusable option — otherwise `Union[Leaf, SomeTypedDict]` would false-reject a
legitimate dict.

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

**Known, measured, still open:** the wrap/plain *validator* ban refuses `Sequence[str]`,
`Deque[str]`, `DefaultDict[str, str]`, `AnyUrl`, `re.Pattern`, `Fraction` and all six `ipaddress`
types, because each compiles to a `function-plain`/`function-wrap` validator node — and the ban
cannot tell an author's wrap validator from one pydantic emits itself. That over-fire predates the value-first check
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
* **Re-checking the numeric tower.** Scoping the runtime-class check to model positions gives up
  exactly five widenings, enumerated by sweeping twelve scalar annotations against thirteen
  values for "strict accepts *and* `isinstance` is False": `float` from `int`, `Decimal` or
  `Fraction`, and `complex` from `int` or `float`. Capacity is not quite nil — a `Decimal` in a
  `float` field carries arbitrary-precision digits, so it is a narrow encoding channel — but
  closing it means disagreeing with pydantic's own `strict=True` about what a `float` field
  admits, which is the inconsistency that made an `int` literal default refused on the default
  path and accepted on the supplied path. Two halves of one gate with different notions of
  "declared type" is a worse defect than the channel.
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
