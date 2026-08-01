"""Immutable recipe registry with derived provenance (issue #145 M12.10).

Deliberately NOT an extension of ``patterns.registry.PatternRegistry``. That one
scans a package at import, keys on a bare name with no version selector, and
carries no provenance — every property this issue needs is one it lacks, and
retrofitting them would change discovery for the six existing archetypes as a
side effect. This registry is a static tuple: a recipe exists because a line of
code registers it, and there is no runtime registration API to call.

Two invariant classes, deliberately reported differently:

* **Build defects** — a duplicate registration, two defaults for one id, an
  ``async`` executor, an undeclared output type — raise ``ValueError`` at
  construction. They are not a caller's problem and must not be reachable as a
  ``RECIPE_*`` code, exactly as ``system_topology.capabilities`` fails its
  coverage check at import rather than at call time.
* **Caller-facing failures** — an unknown id, a missing version, a gated
  capability — raise ``RecipeError`` with the taxonomy code and a value-free
  diagnostic.
"""

from __future__ import annotations

import importlib
import inspect
import json
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ..build_info import (
    PACKAGE_NAME,
    build_metadata,
    implementation_digest,
    package_version,
    source_revision,
)
from ..errors import (
    RECIPE_CAPABILITY_GATED,
    RECIPE_NOT_FOUND,
    RECIPE_REQUEST_INVALID,
    RECIPE_VERSION_UNAVAILABLE,
)
from ..models.recipe_contributions import RECIPE_COMPONENT_TYPES
from .contracts import (
    ExpectedRecipeRegistryV1,
    RecipeCapabilityRequirementV1,
    RecipeDescriptorV1,
    RecipeExecutorV1,
    RecipeImplementationMismatchV1,
    RecipeInputBase,
    RecipeProvenanceV1,
    RecipeRegistrationV1,
    RecipeRegistrySkewV1,
    RecipeVersionMismatchV1,
    parse_semver,
)
from .errors import RecipeError, recipe_error

_CANONICAL = dict(sort_keys=True, separators=(",", ":"), ensure_ascii=True)

#: Positive states, per authority. ``plannable-only`` is satisfied by the
#: stronger ``emittable`` as well — a requirement says "at least this", and a
#: recipe that only needs to PLAN is not broken by an authority that can also
#: emit. Nothing else satisfies anything: ``gated-no-evidence``,
#: ``guidance-only``, ``unsupported`` and *absent* are all failures.
_STATE_SATISFIES: Mapping[str, Tuple[str, ...]] = MappingProxyType(
    {
        "supported": ("supported",),
        "emittable": ("emittable",),
        "plannable-only": ("plannable-only", "emittable"),
    }
)

_ENTRY_KINDS_WITH_EXECUTOR = frozenset({"executable_recipe", "constraint_only"})

#: The modules ``source_revision`` digests — the recipe layer plus its direct
#: callers, NOT everything a recipe run executes. It is the layer-wide backstop
#: the per-entry ``implementation_sha256`` is not; it is not a whole-run digest,
#: and the exclusions block below says exactly what it leaves out.
#:
#: The two hashes answer different questions and are scoped accordingly:
#: ``implementation_sha256`` says WHICH recipe changed (its own defining module);
#: ``source_revision`` says whether ANYTHING in the layer changed. Live QA found
#: this list originally covering only the three executor modules the per-entry
#: digests already covered, plus this one — which no per-entry digest covers at
#: all — so a behaviour-changing edit to ``engine.py`` or ``recipe_bridge.py``
#: moved nothing while both migrated presets went from a working spec to a hard
#: failure. The backstop was documented before it existed (issue #145).
#:
#: The membership rule, narrowed until the prose, the list and the test are the
#: SAME statement. THREE clauses, all three mechanically decidable:
#:
#:   1. every module in the ``recipes`` package;
#:   2. the two CONTRACT modules the layer is built out of —
#:      ``models.recipe_contributions`` (the contribution types) and
#:      ``build_info`` (the provenance derivation) — which are outside the
#:      package only because ``models/`` may not import ``categories/`` and
#:      ``build_info`` must stay stdlib-only;
#:   3. every module that INVOKES the recipe engine.
#:
#: Clause 2 is not a carve-out bolted on to cover two stragglers: it is the same
#: property as clause 1 wearing a different directory. Both modules are #145's
#: own code, both change what a recipe run produces, and both would live in
#: ``recipes/`` if the import rules allowed it. It is named separately because a
#: two-clause version of this sentence covered neither, which live QA measured
#: (issue #145) — the fifth consecutive round in which the prose was narrower or
#: wider than the list it described.
#:
#: "Invokes" is mechanical — a call to ``run_recipes``, ``run_sync_preset_recipe``
#: or ``run_fanout_recipe`` — so ``tests/test_recipe_registry.py`` checks clause 3
#: by walking the package for DIRECT calls, one level, not the transitive
#: closure, rather than by anyone's judgement. One level is the line the digest
#: draws: transitively the reporting layer reaches the engine too, and it is
#: deliberately excluded.
#:
#: That narrowing is itself the fix for a recurring failure. Four consecutive
#: rounds of live QA falsified a *broader* sentence than the list backed, each
#: time by finding a module the words covered and the digest did not. The problem
#: was never the missing module; it was writing a claim no test could check. A
#: rule an AST scan can decide cannot drift from its list.
#:
#: **What is deliberately OUTSIDE, and why.**
#:
#: * ~30 downstream canonical modules — ``compiler/process_ir/*``,
#:   ``models/process_ir``, the component builders, the graph verifier. A recipe
#:   run executes them, and editing one CAN change the emitted XML with nothing
#:   published moving (round 4 of live QA measured it: a ``sync_pipeline`` adapter
#:   label change altered the emitted ``userlabel`` on three of the five emitted
#:   process shapes, snapshot byte-identical).
#: * The REPORTING layer — ``categories/integration_authoring``,
#:   ``integration_import``, ``meta_tools``. They read the registry to build a
#:   response and never invoke the engine DIRECTLY (transitively they reach it
#:   through ``composition.py``, which is listed). An edit there can change
#:   published bytes without changing any recipe's output.
#: * The two package ``__init__.py`` shims (``recipes`` and ``recipes.builtins``).
#:   They are pure re-exports with no logic; the modules they re-export are all
#:   listed. The engine-invocation pin DOES scan ``__init__.py`` files, so one
#:   that ever gained a call would be caught.
#:
#: All three are excluded on purpose. Folding the first two in would make this "a
#: hash of the package rather than of a recipe" — the exact thing ``_implementation_identity``
#: refuses one screen down — and would move every recipe's revision on any
#: compiler or response-shape change. The downstream modules are separately
#: versioned canonical authorities with their own published capability manifests,
#: which ``capability_revisions`` tracks; a deployed image's ``$COMMIT_SHA``
#: covers the whole tree regardless.
#:
#: **In a source checkout, a change to an excluded module is not visible in**
#: ``source_revision``. A real, stated bound. Earlier revisions of this comment
#: claimed coverage that did not exist — twice — and an honest boundary is worth
#: more than an ever-widening digest.
#:
#: STATIC, not a package walk: a scan would make the digest a property of the
#: filesystem, which is the exact failure mode this layer avoids for discovery.
#: The IMPORT prefix of the namespace this module was actually loaded under —
#: ``boomi_mcp`` or ``src.boomi_mcp``. Distinct from ``PACKAGE_NAME``, which is
#: the DISTRIBUTION name and stays ``boomi_mcp`` in published provenance.
#:
#: The repo supports both namespaces (``errors.py`` is stdlib-only for exactly
#: that reason), and hard-coding the distribution name here made
#: ``source_digest`` import ``boomi_mcp.*`` from a checkout loaded as
#: ``src.boomi_mcp.*`` — a ``ModuleNotFoundError`` that took the whole registry
#: down at first use (issue #145, Codex review).
_IMPORT_PREFIX = __name__.rsplit(".recipes.registry", 1)[0]

RECIPE_LAYER_MODULES: Tuple[str, ...] = (
    f"{_IMPORT_PREFIX}.build_info",
    f"{_IMPORT_PREFIX}.models.recipe_contributions",
    # Migrated surfaces — they call the bridge, so they are in the path.
    f"{_IMPORT_PREFIX}.patterns.archetypes.api_to_api_sync",
    f"{_IMPORT_PREFIX}.patterns.archetypes.api_to_database_sync",
    f"{_IMPORT_PREFIX}.patterns.composition",
    f"{_IMPORT_PREFIX}.patterns.recipe_bridge",
    f"{_IMPORT_PREFIX}.recipes.builtins.catalog",
    f"{_IMPORT_PREFIX}.recipes.builtins.fanout",
    f"{_IMPORT_PREFIX}.recipes.builtins.sync",
    f"{_IMPORT_PREFIX}.recipes.composer",
    f"{_IMPORT_PREFIX}.recipes.contracts",
    f"{_IMPORT_PREFIX}.recipes.engine",
    f"{_IMPORT_PREFIX}.recipes.errors",
    f"{_IMPORT_PREFIX}.recipes.materialization",
    f"{_IMPORT_PREFIX}.recipes.registry",
)


def _canonical(value: Any) -> str:
    return json.dumps(value, **_CANONICAL)


# ---------------------------------------------------------------------------
# Capability authorities
# ---------------------------------------------------------------------------


def _process_ir_states() -> Mapping[str, str]:
    from ..models.process_ir import PROCESS_IR_V1_CAPABILITIES

    return PROCESS_IR_V1_CAPABILITIES


def _process_body_subjects() -> Tuple[str, ...]:
    from ..compiler.process_ir.body_capabilities import BODY_CAPABILITIES_V1

    return tuple(sorted(f"{owner}.{slot}" for owner, slot in BODY_CAPABILITIES_V1))


def _connector_call_subjects() -> Tuple[str, ...]:
    from ..compiler.process_ir.connector_capabilities import (
        CONNECTOR_CALL_CAPABILITIES_V1,
    )

    return tuple(
        sorted(f"{family}.{action}" for family, action in CONNECTOR_CALL_CAPABILITIES_V1)
    )


def _process_emitter_subjects() -> Tuple[str, ...]:
    from ..compiler.process_ir.emitter_registry import registry_keys

    return tuple(sorted(registry_keys()))


def _topology_states() -> Mapping[str, str]:
    from ..compiler.system_topology.capabilities import SYSTEM_TOPOLOGY_CAPABILITIES

    return {
        subject: entry.state for subject, entry in SYSTEM_TOPOLOGY_CAPABILITIES.items()
    }


def _authority_revision(payload: Any) -> str:
    return implementation_digest((_canonical(payload),))


class RecipeRegistry:
    """An immutable set of registered recipe versions."""

    def __init__(self, registrations: Tuple[RecipeRegistrationV1, ...]) -> None:
        self._descriptors: Dict[Tuple[str, str], RecipeDescriptorV1] = {}
        self._executors: Dict[Tuple[str, str], RecipeExecutorV1] = {}
        self._input_models: Dict[Tuple[str, str], type] = {}
        self._defaults: Dict[str, str] = {}

        # The WHOLE layer, plus any executor module a test registry brought with
        # it. Digesting only the executor modules would make ``source_revision``
        # a restatement of the per-entry hashes rather than a backstop for them.
        modules = sorted(
            set(RECIPE_LAYER_MODULES)
            | {
                reg.executor.__module__
                for reg in registrations
                if reg.executor is not None
            }
        )
        revision = source_revision(modules)

        for registration in registrations:
            self._register(registration, revision)

        # Self-referential capability subjects, checked only now: a recipe may
        # legitimately require one registered LATER in the tuple, so this is the
        # first point at which "does that id exist" has an answer. Without it the
        # docstring's promise — a typo fails at construction rather than blaming
        # the caller's platform at preflight — held for six authorities and not
        # the seventh (issue #145, live QA).
        for descriptor in self._descriptors.values():
            for prerequisite in descriptor.prerequisites:
                # A self-dependency builds cleanly and then raises a BARE
                # ValueError from the composer's cycle guard, outside the
                # RecipeError envelope (issue #145, live QA). It is a build
                # defect, so it belongs here.
                if (
                    getattr(prerequisite, "kind", None) == "recipe"
                    and prerequisite.recipe_id == descriptor.recipe_id
                    and prerequisite.recipe_version == descriptor.recipe_version
                ):
                    raise ValueError(
                        f"{descriptor.recipe_id!r} declares itself as a prerequisite"
                    )
            for requirement in descriptor.capability_requirements:
                if requirement.authority != "recipe_registry":
                    continue
                if not self.versions_for(requirement.subject):
                    raise ValueError(
                        f"{descriptor.recipe_id!r} requires unknown recipe_registry "
                        f"subject {requirement.subject!r}"
                    )

        self._registry_revision = self._compute_registry_revision()
        self._source_revision = revision

    # -- construction ------------------------------------------------------

    def _register(self, reg: RecipeRegistrationV1, revision: str) -> None:
        key = (reg.recipe_id, reg.recipe_version)
        if key in self._descriptors:
            raise ValueError(
                f"duplicate recipe registration for {reg.recipe_id}@{reg.recipe_version}"
            )
        try:
            parse_semver(reg.recipe_version)
        except ValueError as exc:
            raise ValueError(str(exc)) from None

        self._check_entry_kind(reg)

        module, symbol, impl_parts = self._implementation_identity(reg)
        input_schema_id: Optional[str] = None
        input_schema_sha: Optional[str] = None
        if reg.input_model is not None:
            schema_json = _canonical(reg.input_model.model_json_schema())
            input_schema_id = (
                f"{reg.input_model.__module__}.{reg.input_model.__qualname__}"
            )
            input_schema_sha = implementation_digest((schema_json,))
            impl_parts = impl_parts + (schema_json,)

        implementation_sha256 = implementation_digest(impl_parts)

        descriptor_body = {
            "schema_version": "1",
            "recipe_id": reg.recipe_id,
            "recipe_version": reg.recipe_version,
            "is_default": reg.is_default,
            "entry_kind": reg.entry_kind,
            "input_schema_id": input_schema_id,
            "input_schema_sha256": input_schema_sha,
            "output_types": list(reg.output_types),
            "prerequisites": [p.model_dump(mode="json") for p in reg.prerequisites],
            "capability_requirements": [
                c.model_dump(mode="json") for c in reg.capability_requirements
            ],
            "conflict_policy": (
                reg.conflict_policy.model_dump(mode="json")
                if reg.conflict_policy is not None
                else None
            ),
            "adapter_target": (
                reg.adapter_target.model_dump(mode="json")
                if reg.adapter_target is not None
                else None
            ),
            "provenance_module": module,
            "provenance_symbol": symbol,
            "implementation_sha256": implementation_sha256,
        }
        # ``descriptor_sha256`` hashes the body WITHOUT itself. A self-referential
        # hash is not merely awkward to compute — it is unverifiable, because a
        # reader cannot reproduce it without already knowing the answer.
        descriptor_sha256 = implementation_digest((_canonical(descriptor_body),))

        descriptor = RecipeDescriptorV1(
            recipe_id=reg.recipe_id,
            recipe_version=reg.recipe_version,
            is_default=reg.is_default,
            entry_kind=reg.entry_kind,
            input_schema_id=input_schema_id,
            input_schema_sha256=input_schema_sha,
            output_types=tuple(reg.output_types),
            prerequisites=tuple(reg.prerequisites),
            capability_requirements=tuple(reg.capability_requirements),
            conflict_policy=reg.conflict_policy,
            adapter_target=reg.adapter_target,
            provenance=RecipeProvenanceV1(
                package_name=PACKAGE_NAME,
                package_version=package_version(),
                source_revision=revision,
                module=module,
                symbol=symbol,
                implementation_sha256=implementation_sha256,
                descriptor_sha256=descriptor_sha256,
            ),
        )

        if reg.is_default:
            if reg.recipe_id in self._defaults:
                raise ValueError(
                    f"more than one default version declared for {reg.recipe_id}"
                )
            self._defaults[reg.recipe_id] = reg.recipe_version

        self._descriptors[key] = descriptor
        if reg.executor is not None:
            self._executors[key] = reg.executor
        if reg.input_model is not None:
            self._input_models[key] = reg.input_model

        self._check_capability_subjects(descriptor)
        self._check_capability_states(descriptor)

    def _check_entry_kind(self, reg: RecipeRegistrationV1) -> None:
        kind = reg.entry_kind
        has_executor = reg.executor is not None
        has_input = reg.input_model is not None

        if kind in _ENTRY_KINDS_WITH_EXECUTOR:
            if not has_executor or not has_input:
                raise ValueError(
                    f"{kind} {reg.recipe_id!r} requires both an executor and an input model"
                )
            if not reg.output_types:
                raise ValueError(
                    f"{kind} {reg.recipe_id!r} must declare at least one output type"
                )
            if reg.adapter_target is not None:
                raise ValueError(f"{kind} {reg.recipe_id!r} must not name an adapter target")
            if reg.conflict_policy is None:
                raise ValueError(f"{kind} {reg.recipe_id!r} must declare a conflict policy")
            if kind == "constraint_only" and set(reg.output_types) != {
                "constraint_requirement"
            }:
                raise ValueError(
                    f"constraint_only {reg.recipe_id!r} may declare only "
                    "constraint_requirement output"
                )
        elif kind == "advisory":
            # The enforcement half of "doctrine cannot become executable".
            # ``RecipeRegistrationV1`` is ONE dataclass for all four kinds, so an
            # ``executor`` field does exist — this is what rejects an advisory
            # entry that populates it, before the registry is usable. What holds
            # regardless of any field is that prose is never parsed, so there is
            # no parser to trick.
            if has_executor or has_input or reg.output_types or reg.adapter_target:
                raise ValueError(
                    f"advisory {reg.recipe_id!r} must declare no executor, input "
                    "model, outputs or adapter target"
                )
            if reg.conflict_policy is not None:
                raise ValueError(f"advisory {reg.recipe_id!r} must declare no conflict policy")
        elif kind == "compatibility_adapter":
            if has_executor or reg.output_types:
                raise ValueError(
                    f"compatibility_adapter {reg.recipe_id!r} must declare no "
                    "executor and no contribution output"
                )
            if reg.adapter_target is None:
                raise ValueError(
                    f"compatibility_adapter {reg.recipe_id!r} must name an exact adapter target"
                )
        else:  # pragma: no cover - the Literal already bounds this
            raise ValueError(f"unknown recipe entry kind {kind!r}")

        if has_input and not (
            isinstance(reg.input_model, type)
            and issubclass(reg.input_model, RecipeInputBase)
        ):
            raise ValueError(
                f"{reg.recipe_id!r} input_model must subclass RecipeInputBase"
            )

        if has_executor:
            self._check_executor_shape(reg)

        for output in reg.output_types:
            if output not in {
                "process_ir_patch",
                "system_topology_patch",
                "component_contribution",
                "constraint_requirement",
            }:
                raise ValueError(
                    f"{reg.recipe_id!r} declares unknown output type {output!r}"
                )
        if len(set(reg.output_types)) != len(reg.output_types):
            raise ValueError(f"{reg.recipe_id!r} declares a duplicate output type")
        if list(reg.output_types) != sorted(reg.output_types):
            raise ValueError(f"{reg.recipe_id!r} output_types must be canonically sorted")

    @staticmethod
    def _check_executor_shape(reg: RecipeRegistrationV1) -> None:
        """Only a plain module-level function may be a recipe executor.

        A coroutine cannot be run twice and byte-compared without an event loop
        the engine does not own. A lambda, a ``functools.partial`` and a bound
        method all carry captured state whose source ``inspect.getsource``
        cannot see — so their ``implementation_sha256`` would be blind to the
        very thing that changes their behavior, and the skew report would say
        ``match`` for two registries running different code.
        """
        executor = reg.executor
        if inspect.iscoroutinefunction(executor) or inspect.isasyncgenfunction(executor):
            # BOTH. ``iscoroutinefunction`` is False for ``async def f(): yield``
            # while ``isfunction`` is True, so an async generator slipped through
            # construction and surfaced at run time as
            # ``RECIPE_CONTRIBUTION_INVALID`` — a build defect wearing a
            # caller-facing code (issue #145, live QA).
            raise ValueError(f"{reg.recipe_id!r} executor must not be a coroutine function")
        if inspect.isgeneratorfunction(executor):
            # A generator returns a generator, never the declared tuple.
            raise ValueError(f"{reg.recipe_id!r} executor must not be a generator function")
        if not inspect.isfunction(executor):
            raise ValueError(
                f"{reg.recipe_id!r} executor must be a plain module-level function"
            )
        if getattr(executor, "__name__", "") == "<lambda>":
            raise ValueError(f"{reg.recipe_id!r} executor must not be a lambda")
        if getattr(executor, "__closure__", None):
            raise ValueError(f"{reg.recipe_id!r} executor must not close over state")
        qualname = getattr(executor, "__qualname__", "")
        if "." in qualname or "<locals>" in qualname:
            raise ValueError(
                f"{reg.recipe_id!r} executor must be defined at module level"
            )

    @staticmethod
    def _implementation_identity(
        reg: RecipeRegistrationV1,
    ) -> Tuple[str, str, Tuple[str, ...]]:
        """``(module, symbol, hash parts)`` derived from the registered code.

        Hashes the registered function's own source **and its defining module's**.

        The function alone is not enough, and that is not a theoretical gap: both
        sync recipes delegate their entire body to a shared ``_contributions``
        helper, so a behaviour-changing edit to that helper moved NO hash at all
        while ``build_from_archetype`` went from a working spec to a hard failure.
        A caller comparing implementation hashes would have been told ``match``
        about a registry whose output had changed — exactly the false parity this
        field exists to prevent. (Live QA, issue #145.)

        Including the whole module is deliberately conservative: an unrelated edit
        elsewhere in the module moves the hash too. That direction is the safe
        one — a spurious ``mismatch`` prompts a look, a spurious ``match`` ends
        the investigation. Per-symbol attribution survives because each
        registration still hashes its own source alongside the module's, so two
        recipes sharing a module remain distinguishable.

        A helper in a DIFFERENT module is still outside the digest. That bound is
        stated rather than papered over: chasing the full import closure would
        make every recipe's hash move on any change anywhere in the package,
        which is a hash of the package, not of a recipe. The
        ``source_revision``/``registry_revision`` round-trip is what covers that
        wider surface, and ``compare`` now refuses to report a bare ``match``
        when neither was supplied.

        Fails CLOSED when the source cannot be read: an empty or defaulted hash
        would make every skew comparison report ``match``, which is worse than
        refusing to start.
        """
        if reg.executor is None:
            # An adapter or advisory entry has no code of its own — it IS a
            # declaration. So the digest covers the declaration's actual source:
            # the catalog module where it is written, plus the adapter target it
            # names. Live QA found the original form hashed only
            # ``(id, version, entry_kind)``, which never moved for ANY edit —
            # including one to the catalog module itself — so four of eight
            # published entries pinned nothing, in a field the skew note tells
            # callers to rely on (issue #145).
            #
            # The adapter's runtime behaviour lives in ``patterns/recipe_bridge``
            # and the migrated archetype modules, which no single entry can own.
            # That is what ``source_revision`` is for, and ``RECIPE_LAYER_MODULES``
            # lists every one of them — including the migrated surfaces, which
            # round 3 of live QA found missing from an earlier version of this
            # same sentence.
            catalog_module = f"{_IMPORT_PREFIX}.recipes.builtins.catalog"
            try:
                catalog_source = inspect.getsource(
                    importlib.import_module(catalog_module)
                )
            except (OSError, TypeError, ImportError) as exc:  # pragma: no cover
                raise ValueError(
                    "recipe provenance requires readable source for "
                    f"{catalog_module}"
                ) from exc
            target = (
                _canonical(reg.adapter_target.model_dump(mode="json"))
                if reg.adapter_target is not None
                else ""
            )
            return (
                catalog_module,
                reg.recipe_id,
                (
                    catalog_module,
                    reg.recipe_id,
                    reg.recipe_version,
                    reg.entry_kind,
                    target,
                    catalog_source,
                ),
            )
        module = reg.executor.__module__
        symbol = reg.executor.__qualname__
        try:
            source = inspect.getsource(reg.executor)
            module_source = inspect.getsource(inspect.getmodule(reg.executor))
        except (OSError, TypeError) as exc:  # pragma: no cover - environment
            raise ValueError(
                f"recipe provenance requires readable source for "
                f"{module}.{symbol}"
            ) from exc
        return module, symbol, (module, symbol, source, module_source)

    #: The states a STATEFUL authority can actually report. A required_state that
    #: no state here satisfies is unsatisfiable by construction — e.g.
    #: ``plannable-only`` against ``process_ir``, which only ever reports
    #: supported/gated/unsupported. Caught at construction rather than surfacing
    #: as ``RECIPE_CAPABILITY_GATED``, which would blame the caller's platform for
    #: our impossible declaration (issue #145, live QA).
    _STATEFUL_AUTHORITY_STATES: Mapping[str, frozenset] = MappingProxyType(
        {
            "process_ir": frozenset({"supported", "gated", "unsupported"}),
            "system_topology": frozenset(
                {
                    "emittable",
                    "plannable-only",
                    "guidance-only",
                    "gated-no-evidence",
                    "unsupported",
                }
            ),
        }
    )

    def _check_capability_states(self, descriptor: RecipeDescriptorV1) -> None:
        for requirement in descriptor.capability_requirements:
            possible = self._STATEFUL_AUTHORITY_STATES.get(requirement.authority)
            if possible is None:
                continue  # a membership authority; presence satisfies any ask
            accepted = _STATE_SATISFIES[requirement.required_state]
            if not (possible & set(accepted)):
                raise ValueError(
                    f"{descriptor.recipe_id!r} requires "
                    f"{requirement.required_state!r} of {requirement.authority!r}, "
                    "which can never report it"
                )

    def _check_capability_subjects(self, descriptor: RecipeDescriptorV1) -> None:
        """A capability requirement must NAME something the authority knows.

        Enforced at construction, not at call time: a typo'd subject would
        otherwise sit dormant and then surface as ``RECIPE_CAPABILITY_GATED``,
        telling a caller their platform lacks a feature when in fact our
        registration is misspelled.
        """
        for requirement in descriptor.capability_requirements:
            known = self._authority_subjects(requirement.authority)
            if known is not None and requirement.subject not in known:
                raise ValueError(
                    f"{descriptor.recipe_id!r} requires unknown "
                    f"{requirement.authority} subject {requirement.subject!r}"
                )

    def _authority_subjects(self, authority: str) -> Optional[frozenset]:
        if authority == "process_ir":
            return frozenset(_process_ir_states())
        if authority == "process_body":
            return frozenset(_process_body_subjects())
        if authority == "connector_call":
            return frozenset(_connector_call_subjects())
        if authority == "process_emitter":
            return frozenset(_process_emitter_subjects())
        if authority == "system_topology":
            return frozenset(_topology_states())
        if authority == "component_builder":
            return frozenset(RECIPE_COMPONENT_TYPES)
        if authority == "recipe_registry":
            # Self-reference: a recipe may require one registered LATER in the
            # tuple, so per-registration checking cannot answer it. ``None`` defers
            # to the whole-registry sweep at the end of ``__init__`` — deferred,
            # not skipped.
            return None
        return frozenset()  # pragma: no cover - Literal-bounded

    def _compute_registry_revision(self) -> str:
        payload = [
            descriptor.model_dump(mode="json")
            for descriptor in self.descriptors()
        ]
        return implementation_digest((_canonical(payload),))

    # -- discovery ---------------------------------------------------------

    def descriptors(self) -> Tuple[RecipeDescriptorV1, ...]:
        """Every descriptor, in the canonical discovery order.

        Sorted by id, then parsed SemVer, then entry kind — parsed, not
        lexical, because ``1.10.0`` must sort after ``1.9.0``. The sort makes
        discovery independent of registration order, which is what lets the
        registry revision be a stable identity rather than an accident of
        import sequence.
        """
        # ``recipe_version`` is the final tie-break, not just ``parse_semver`` of
        # it: build metadata is ignored FOR PRECEDENCE by SemVer §10, so
        # ``1.0.0+a`` and ``1.0.0+b`` compare equal and their order — and hence
        # ``registry_revision`` — would fall to dict insertion order
        # (issue #145, live QA). The raw string breaks the tie deterministically.
        return tuple(
            sorted(
                self._descriptors.values(),
                key=lambda d: (
                    d.recipe_id,
                    parse_semver(d.recipe_version),
                    d.entry_kind,
                    d.recipe_version,
                ),
            )
        )

    @property
    def registry_revision(self) -> str:
        return self._registry_revision

    @property
    def source_revision_value(self) -> str:
        return self._source_revision

    def versions_for(self, recipe_id: str) -> Tuple[str, ...]:
        return tuple(
            sorted(
                (rid_version for rid, rid_version in self._descriptors if rid == recipe_id),
                key=lambda v: (parse_semver(v), v),
            )
        )

    def resolve(
        self, recipe_id: str, recipe_version: Optional[str] = None
    ) -> RecipeDescriptorV1:
        """Resolve an exact version, or the code-declared default.

        An exact request NEVER falls forward or backward. A caller who asked for
        ``1.2.0`` and silently got ``1.3.0`` would have no way to notice, which
        defeats the entire point of pinning.
        """
        available = self.versions_for(recipe_id)
        if not available:
            raise recipe_error(RECIPE_NOT_FOUND, phase="lookup", recipe_ids=(recipe_id,))
        if recipe_version is None:
            default = self._defaults.get(recipe_id)
            if default is None:
                raise recipe_error(
                    RECIPE_VERSION_UNAVAILABLE,
                    phase="lookup",
                    recipe_ids=(recipe_id,),
                    available_versions=available,
                )
            return self._descriptors[(recipe_id, default)]
        descriptor = self._descriptors.get((recipe_id, recipe_version))
        if descriptor is None:
            raise recipe_error(
                RECIPE_VERSION_UNAVAILABLE,
                phase="lookup",
                recipe_ids=(recipe_id,),
                recipe_versions=(recipe_version,),
                available_versions=available,
            )
        return descriptor

    def executor_for(self, descriptor: RecipeDescriptorV1) -> RecipeExecutorV1:
        key = (descriptor.recipe_id, descriptor.recipe_version)
        executor = self._executors.get(key)
        if executor is None:
            # It IS registered — as an advisory pointer or a compatibility
            # adapter. Reporting "no registered recipe carries the requested id"
            # sent a caller looking for a typo in a name that was correct
            # (issue #145, live QA). The entry kind is the answer.
            raise recipe_error(
                RECIPE_REQUEST_INVALID,
                phase="lookup",
                target=f"not_executable:{descriptor.entry_kind}",
                recipe_ids=(descriptor.recipe_id,),
                recipe_versions=(descriptor.recipe_version,),
            )
        return executor

    def input_model_for(self, descriptor: RecipeDescriptorV1) -> type:
        key = (descriptor.recipe_id, descriptor.recipe_version)
        model = self._input_models.get(key)
        if model is None:
            raise recipe_error(
                RECIPE_REQUEST_INVALID,
                phase="lookup",
                target=f"not_executable:{descriptor.entry_kind}",
                recipe_ids=(descriptor.recipe_id,),
                recipe_versions=(descriptor.recipe_version,),
            )
        return model

    # -- capability preflight ---------------------------------------------

    def preflight_capabilities(self, descriptor: RecipeDescriptorV1) -> None:
        """Raise ``RECIPE_CAPABILITY_GATED`` for any unmet requirement."""
        for requirement in descriptor.capability_requirements:
            if not self._capability_satisfied(requirement):
                # The AUTHORITY only. Its subject names a key inside a canonical
                # authority, and for ``process_emitter`` those are dark compiler
                # internals — the same ones ``public_payload`` redacts. Publishing
                # them here would have leaked through the diagnostic exactly what
                # the descriptor projection withholds (issue #145, live QA).
                raise recipe_error(
                    RECIPE_CAPABILITY_GATED,
                    phase="capability",
                    target=f"capability:{requirement.authority}",
                    recipe_ids=(descriptor.recipe_id,),
                    recipe_versions=(descriptor.recipe_version,),
                )

    def capability_satisfied(self, requirement: RecipeCapabilityRequirementV1) -> bool:
        """Whether one capability requirement holds.

        Public because the engine's ``RequireCapability`` CONSTRAINT asks exactly
        this question at runtime; reaching through a private name would make the
        two answers separable.

        That runtime path gets the same two guards a DESCRIPTOR requirement gets
        at construction — an unknown subject and an unsatisfiable
        ``(authority, required_state)`` pair. Live QA found it getting neither,
        so a contributed requirement naming a typo'd subject reported
        "not satisfied" rather than "you asked for something that does not
        exist", and an impossible state pair reported the same (issue #145).
        Both are ``False`` here rather than exceptions: a contribution is
        caller-adjacent input, so an unanswerable requirement is an unmet
        requirement, and the engine turns it into ``RECIPE_CONSTRAINT_FAILED``.
        """
        known = self._authority_subjects(requirement.authority)
        if known is not None and requirement.subject not in known:
            return False
        if requirement.authority == "recipe_registry" and not self.versions_for(
            requirement.subject
        ):
            return False
        possible = self._STATEFUL_AUTHORITY_STATES.get(requirement.authority)
        if possible is not None and not (
            possible & set(_STATE_SATISFIES[requirement.required_state])
        ):
            return False
        return self._capability_satisfied(requirement)

    def _capability_satisfied(self, requirement: RecipeCapabilityRequirementV1) -> bool:
        accepted = _STATE_SATISFIES[requirement.required_state]
        authority = requirement.authority
        if authority == "process_ir":
            return _process_ir_states().get(requirement.subject) in accepted
        if authority == "system_topology":
            return _topology_states().get(requirement.subject) in accepted
        # The membership authorities have no per-subject state: a subject is
        # either registered — which IS its strongest state — or absent.
        if authority == "process_body":
            present = requirement.subject in _process_body_subjects()
        elif authority == "connector_call":
            present = requirement.subject in _connector_call_subjects()
        elif authority == "process_emitter":
            present = requirement.subject in _process_emitter_subjects()
        elif authority == "component_builder":
            present = requirement.subject in RECIPE_COMPONENT_TYPES
        elif authority == "recipe_registry":
            present = bool(self.versions_for(requirement.subject))
        else:  # pragma: no cover - Literal-bounded
            present = False
        # A registered subject satisfies ANY positive state, not only
        # ``supported``. The earlier form was ``present and "supported" in
        # accepted``, which inverted the "at least this" rule on five of the
        # seven authorities: a ``plannable-only`` requirement — the WEAKEST ask —
        # could never be satisfied, because ``_STATE_SATISFIES["plannable-only"]``
        # deliberately contains no ``supported``. Latent while every built-in
        # asks for ``supported``, and a live-QA mutant read inert for exactly
        # that reason (issue #145). A membership registry answers "is it there",
        # so presence is the ceiling and every positive ask is under it.
        return present and bool(accepted)

    def capability_revisions(self) -> Mapping[str, str]:
        from ..compiler.system_topology.capabilities import (
            SYSTEM_TOPOLOGY_CAPABILITY_REVISION,
        )

        return {
            "process_ir": _authority_revision(dict(_process_ir_states())),
            "process_body": _authority_revision(list(_process_body_subjects())),
            "connector_call": _authority_revision(list(_connector_call_subjects())),
            "process_emitter": _authority_revision(list(_process_emitter_subjects())),
            "system_topology": SYSTEM_TOPOLOGY_CAPABILITY_REVISION,
            "component_builder": _authority_revision(list(RECIPE_COMPONENT_TYPES)),
        }

    # -- snapshot + skew ---------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """The published registry snapshot, fully sorted and value-free."""
        metadata = build_metadata([__name__])
        return {
            "schema_version": "1",
            "registry_revision": self.registry_revision,
            "source_version": metadata["package_version"],
            "source_revision": self._source_revision,
            "source_revision_kind": metadata["source_revision_kind"],
            "capability_revisions": dict(sorted(self.capability_revisions().items())),
            "entries": [
                {
                    "recipe_id": d.recipe_id,
                    "recipe_version": d.recipe_version,
                    "entry_kind": d.entry_kind,
                    "is_default": d.is_default,
                    "output_types": list(d.output_types),
                    "implementation_sha256": d.provenance.implementation_sha256,
                    "descriptor_sha256": d.provenance.descriptor_sha256,
                    "adapter_target": (
                        d.adapter_target.model_dump(mode="json")
                        if d.adapter_target is not None
                        else None
                    ),
                }
                for d in self.descriptors()
            ],
        }

    def compare(
        self, expected: Optional[ExpectedRecipeRegistryV1]
    ) -> RecipeRegistrySkewV1:
        """Compare a caller's expectation against the live registry.

        Equal semantic versions are never treated as proof of equal code: when
        the caller supplied an ``implementation_sha256`` it is compared, and a
        difference is a ``mismatch`` even though the versions agree. That is the
        skew the issue's research note is actually about.
        """
        if expected is None:
            return RecipeRegistrySkewV1(status="not_requested")

        live = {
            (d.recipe_id, d.recipe_version): d
            for d in self.descriptors()
        }
        live_ids = {rid for rid, _ in live}
        expected_ids = {entry.recipe_id for entry in expected.entries}

        missing: List[str] = []
        version_mismatches: List[RecipeVersionMismatchV1] = []
        implementation_mismatches: List[RecipeImplementationMismatchV1] = []

        for entry in expected.entries:
            if entry.recipe_id not in live_ids:
                missing.append(entry.recipe_id)
                continue
            descriptor = live.get((entry.recipe_id, entry.recipe_version))
            if descriptor is None:
                live_versions = self.versions_for(entry.recipe_id)
                version_mismatches.append(
                    RecipeVersionMismatchV1(
                        recipe_id=entry.recipe_id,
                        expected_version=entry.recipe_version,
                        live_version=",".join(live_versions),
                    )
                )
                continue
            if (
                entry.implementation_sha256 is not None
                and entry.implementation_sha256
                != descriptor.provenance.implementation_sha256
            ):
                implementation_mismatches.append(
                    RecipeImplementationMismatchV1(
                        recipe_id=entry.recipe_id,
                        recipe_version=entry.recipe_version,
                        expected_implementation_sha256=entry.implementation_sha256,
                        live_implementation_sha256=(
                            descriptor.provenance.implementation_sha256
                        ),
                    )
                )

        # By (id, VERSION), not id. An id-only subtraction is blind to an extra
        # VERSION of a known recipe: a live registry carrying x@1.0.0 AND x@2.0.0
        # reported ``match`` against a fully-hashed expectation naming only
        # x@1.0.0, which is precisely the parallel-version support this registry
        # advertises (issue #145, Codex review).
        expected_pairs = {
            (entry.recipe_id, entry.recipe_version) for entry in expected.entries
        }
        live_only = sorted(
            f"{recipe_id}@{version}"
            for recipe_id, version in live
            if (recipe_id, version) not in expected_pairs
            # An id absent from the expectation ENTIRELY is not "extra" — the
            # caller never claimed to know about it, and reporting every version
            # of it would drown the real finding.
            and recipe_id in expected_ids
        ) + sorted(
            recipe_id for recipe_id in (live_ids - expected_ids)
        )
        registry_revision_mismatch = bool(
            expected.registry_revision
            and expected.registry_revision != self.registry_revision
        )
        source_revision_mismatch = bool(
            expected.source_revision and expected.source_revision != self._source_revision
        )

        mismatched = (
            missing
            or live_only
            or version_mismatches
            or implementation_mismatches
            or registry_revision_mismatch
            or source_revision_mismatch
        )

        # A comparison that could not establish parity must SAY so. An
        # expectation carrying only recipe ids and versions proves nothing about
        # the code — two registries can agree on every version and run different
        # bytes, which is the whole reason this comparison exists. Reporting a
        # bare ``match`` for it is the "silent looks-fine" that
        # ``RecipeRegistrySkewV1`` explicitly forbids, and it is what live QA
        # caught (issue #145).
        #
        # Parity IS establishable when either revision was supplied (each is a
        # digest over everything) or when every entry carried an implementation
        # hash. Anything less is ``unknown`` with a reason, never ``match``.
        # ``mismatch`` is unaffected: a difference found is a real finding no
        # matter how partial the comparison was.
        revisions_supplied = bool(expected.registry_revision or expected.source_revision)
        entries_fully_hashed = bool(expected.entries) and all(
            entry.implementation_sha256 is not None for entry in expected.entries
        )
        if not mismatched and not (revisions_supplied or entries_fully_hashed):
            return RecipeRegistrySkewV1(
                status="unknown",
                reason=(
                    "partial_comparison: every expected id and version matched, but "
                    "neither registry_revision nor source_revision was supplied and "
                    "not every entry carried implementation_sha256 — equal versions "
                    "are not evidence of equal code"
                ),
                live_only=tuple(live_only),
            )

        return RecipeRegistrySkewV1(
            status="mismatch" if mismatched else "match",
            missing_from_live=tuple(sorted(missing)),
            live_only=tuple(live_only),
            version_mismatches=tuple(
                sorted(version_mismatches, key=lambda m: m.recipe_id)
            ),
            implementation_mismatches=tuple(
                sorted(
                    implementation_mismatches,
                    key=lambda m: (m.recipe_id, m.recipe_version),
                )
            ),
            registry_revision_mismatch=registry_revision_mismatch,
            source_revision_mismatch=source_revision_mismatch,
        )


def build_test_registry(
    registrations: Tuple[RecipeRegistrationV1, ...]
) -> RecipeRegistry:
    """Construct an ISOLATED registry. Test-only.

    Deliberately a separate factory rather than a ``register()`` method on the
    production registry: a mutation method would exist in production too, and
    "there is no runtime registration API" is a property a test asserts by
    checking the module, not one anybody can promise in a docstring.
    """
    return RecipeRegistry(registrations)


def _production_registry() -> RecipeRegistry:
    from .builtins.catalog import PRODUCTION_REGISTRATIONS

    return RecipeRegistry(PRODUCTION_REGISTRATIONS)


_PRODUCTION_REGISTRY: Optional[RecipeRegistry] = None


def production_registry() -> RecipeRegistry:
    """The single production registry, built once.

    Lazily, because the built-ins import contribution models which import the
    topology models — building at module import would make ``recipes.registry``
    unimportable from inside that cycle.
    """
    global _PRODUCTION_REGISTRY
    if _PRODUCTION_REGISTRY is None:
        _PRODUCTION_REGISTRY = _production_registry()
    return _PRODUCTION_REGISTRY


__all__ = [
    "RecipeRegistry",
    "build_test_registry",
    "production_registry",
]
