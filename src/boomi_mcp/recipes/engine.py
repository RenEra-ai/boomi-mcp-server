"""The recipe execution funnel (issue #145 M12.10).

Every recipe run walks the SAME twelve steps, and none of them is optional:

 1. resolve the descriptor at an exact version
 2. preflight its declared capability requirements
 3. preflight its declared prerequisites — recipe AND execution-context
 4. pre-scan and strictly validate its input
 5. run ONLY the registered callable
 6. re-validate every returned value as a DECLARED contribution type
 7. run it a second time and byte-compare — nondeterminism is a hard failure
 8. order and compose the closed operations
 9. resolve component slots against the private catalog, verifying headers
10. parse / compile / emit / verify every assembled process
11. plan every assembled topology
12. evaluate every declared constraint

The artifacts then go back to the caller. Mutation still goes through
``_build_plan`` / ``_apply_plan``, which this module never calls — those are not
steps of the funnel, they are what happens after it.

**``validation_policy`` is hard-coded ``None`` at the compile call site and there
is no parameter anywhere in this module that could carry one.** That is what makes
"a recipe cannot bypass semantic validation" a structural fact instead of a
convention: the legacy exemptions exist and are reachable, but not from here, and
a test asserts the signature has no seam to add one through.

Step 7 is why executors get nothing but their frozen input. A recipe that read a
clock, an environment variable, or a mutable module global would produce different
bytes on the second run and fail closed — but only if there is no legitimate
channel through which state could differ. Handing an executor a context object
would have opened exactly that channel.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import (
    RECIPE_CONSTRAINT_FAILED,
    RECIPE_REQUEST_INVALID,
    RECIPE_CONTRIBUTION_INVALID,
    RECIPE_INPUT_INVALID,
    RECIPE_OUTPUT_NONDETERMINISTIC,
)
from ..models.integration_models import IntegrationComponentSpec
from ..models.recipe_contributions import (
    RecipeContributionValidationError,
    canonical_recipe_contributions_json,
    scan_forbidden_recipe_shape,
    validate_contribution_object,
)
from .composer import (
    AttributedContributionV1,
    ComposedContributionsV1,
    RecipeInvocationV1,
    compose,
    order_invocations,
)
from .contracts import RecipeDescriptorV1
from .errors import RecipeError, recipe_diagnostic, recipe_error
from .materialization import (
    MaterializationCatalog,
    build_symbol_table,
    placeholder_component_id,
)
from .registry import RecipeRegistry, production_registry


@dataclass(frozen=True)
class RecipeRequestV1:
    """One requested recipe run."""

    recipe_id: str
    invocation_id: str
    raw_input: Mapping[str, Any]
    recipe_version: Optional[str] = None


@dataclass(frozen=True)
class RecipeRunResultV1:
    """Everything a validated recipe run produced."""

    composed: ComposedContributionsV1
    components: Tuple[IntegrationComponentSpec, ...]
    process_artifacts: Tuple[Tuple[str, Any], ...]
    topology_plans: Tuple[Tuple[str, Any], ...]
    provenance: Mapping[str, Any]

    def artifact_for(self, process_key: str) -> Any:
        for key, artifact in self.process_artifacts:
            if key == process_key:
                return artifact
        return None


def _validate_input(descriptor: RecipeDescriptorV1, registry: RecipeRegistry, raw: Mapping[str, Any]):
    """Forbidden-shape scan, THEN the strict model.

    The scan runs first so a credential is rejected as a security failure with a
    value-free diagnostic — not as a pydantic schema error whose message could
    echo the offending value into a log.
    """
    if not isinstance(raw, Mapping):
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )
    found = scan_forbidden_recipe_shape(dict(raw))
    if found is not None:
        path, _reason = found
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            path=_pointer(path),
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )
    model = registry.input_model_for(descriptor)
    try:
        return model.model_validate(dict(raw))
    except Exception:  # noqa: BLE001 — pydantic text can echo an authored value
        raise recipe_error(
            RECIPE_INPUT_INVALID,
            phase="input",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        ) from None


def _pointer(path: Tuple[Any, ...]) -> str:
    if not path:
        return "/"
    escaped = [str(part).replace("~", "~0").replace("/", "~1") for part in path]
    return "/" + "/".join(escaped)


def _preflight_prerequisites(
    descriptor: RecipeDescriptorV1,
    registry: RecipeRegistry,
    catalog: MaterializationCatalog,
    topology_context: Any,
) -> None:
    """Resolve every declared recipe prerequisite, or fail.

    ``order_invocations`` deliberately skips a dependency that is not in the
    current invocation set, on the stated grounds that "the engine preflights
    it". Live QA found that the engine did not — the sentence described an
    intention rather than a check, so an unsatisfiable prerequisite ran anyway
    (issue #145). Resolving here makes the claim true and puts the failure where
    a caller can act on it: an exact `(recipe_id, recipe_version)` that is not
    registered is `RECIPE_NOT_FOUND` / `RECIPE_VERSION_UNAVAILABLE`.

    Context prerequisites are checked too, against what the caller actually
    supplied. An earlier version said the engine's signature made them
    structurally satisfied; live QA showed all three kinds running unheld while
    ``ExecutionContextPrerequisiteV1``'s own docstring says the engine "must
    hold" them (issue #145). A ``component_catalog`` needs a non-empty catalog, a
    ``topology_context`` needs one to have been passed, and a
    ``process_symbol_catalog`` needs components to build symbols from — which is
    the same non-empty catalog.
    """
    for prerequisite in descriptor.prerequisites:
        kind = getattr(prerequisite, "kind", None)
        if kind == "recipe":
            registry.resolve(prerequisite.recipe_id, prerequisite.recipe_version)
        elif kind in ("component_catalog", "process_symbol_catalog"):
            if not catalog.slots():
                raise recipe_error(
                    RECIPE_CONSTRAINT_FAILED,
                    phase="capability",
                    target=f"execution_context:{kind}",
                    recipe_ids=(descriptor.recipe_id,),
                    recipe_versions=(descriptor.recipe_version,),
                )
        elif kind == "topology_context":
            if topology_context is None:
                raise recipe_error(
                    RECIPE_CONSTRAINT_FAILED,
                    phase="capability",
                    target="execution_context:topology_context",
                    recipe_ids=(descriptor.recipe_id,),
                    recipe_versions=(descriptor.recipe_version,),
                )


def _run_executor(
    descriptor: RecipeDescriptorV1,
    registry: RecipeRegistry,
    validated_input: Any,
) -> Tuple[Any, ...]:
    """Run the registered callable and strictly re-validate everything it returned."""
    executor = registry.executor_for(descriptor)
    try:
        returned = executor(validated_input)
    except RecipeError:
        raise
    except Exception:  # noqa: BLE001 — an executor message can carry a sentinel
        raise recipe_error(
            RECIPE_CONTRIBUTION_INVALID,
            phase="execution",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        ) from None

    if not isinstance(returned, tuple):
        raise recipe_error(
            RECIPE_CONTRIBUTION_INVALID,
            phase="execution",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )

    declared = set(descriptor.output_types)
    validated: List[Any] = []
    for index, item in enumerate(returned):
        try:
            checked = validate_contribution_object(item)
        except RecipeContributionValidationError:
            raise recipe_error(
                RECIPE_CONTRIBUTION_INVALID,
                phase="execution",
                recipe_ids=(descriptor.recipe_id,),
                recipe_versions=(descriptor.recipe_version,),
                contribution_indexes=(index,),
            ) from None
        if checked.contribution_kind not in declared:
            raise recipe_error(
                RECIPE_CONTRIBUTION_INVALID,
                phase="execution",
                target=f"undeclared_output:{checked.contribution_kind}",
                recipe_ids=(descriptor.recipe_id,),
                recipe_versions=(descriptor.recipe_version,),
                contribution_indexes=(index,),
            )
        validated.append(checked)
    return tuple(validated)


def _execute_deterministically(
    descriptor: RecipeDescriptorV1,
    registry: RecipeRegistry,
    validated_input: Any,
) -> Tuple[Any, ...]:
    first = _run_executor(descriptor, registry, validated_input)
    second = _run_executor(descriptor, registry, validated_input)
    if canonical_recipe_contributions_json(first) != canonical_recipe_contributions_json(
        second
    ):
        raise recipe_error(
            RECIPE_OUTPUT_NONDETERMINISTIC,
            phase="determinism",
            recipe_ids=(descriptor.recipe_id,),
            recipe_versions=(descriptor.recipe_version,),
        )
    return first


def run_recipes(
    requests: Sequence[RecipeRequestV1],
    *,
    catalog: MaterializationCatalog,
    registry: Optional[RecipeRegistry] = None,
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]] = None,
    topology_context: Any = None,
    resolver=placeholder_component_id,
) -> RecipeRunResultV1:
    """Run, compose, and canonically validate a set of recipe requests."""
    active = registry if registry is not None else production_registry()

    seen_invocations = set()
    for request in requests:
        if request.invocation_id in seen_invocations:
            # ``order_invocations`` tracks placement by invocation_id, so a
            # duplicate marks its twin as already placed and can order a recipe
            # BEFORE its declared dependency (issue #145, live QA). The id is the
            # caller's to choose and must be unique.
            # Its OWN code. Reusing RECIPE_INPUT_INVALID told the caller their
            # input carried "credentials, headers, SQL, raw XML" — byte-for-byte
            # the misdiagnosis a sibling fix had just removed (issue #145).
            raise recipe_error(
                RECIPE_REQUEST_INVALID,
                phase="input",
                target="duplicate_invocation_id",
                recipe_ids=(request.recipe_id,),
            )
        seen_invocations.add(request.invocation_id)

    invocations: List[RecipeInvocationV1] = []
    for request in requests:
        descriptor = active.resolve(request.recipe_id, request.recipe_version)
        active.preflight_capabilities(descriptor)
        _preflight_prerequisites(descriptor, active, catalog, topology_context)
        validated_input = _validate_input(descriptor, active, request.raw_input)
        invocations.append(
            RecipeInvocationV1(
                invocation_id=request.invocation_id,
                descriptor=descriptor,
                validated_input=validated_input,
            )
        )

    ordered = order_invocations(invocations)

    attributed: List[AttributedContributionV1] = []
    # Keyed by (id, VERSION). Keying on the id alone let two versions of one
    # recipe share a policy lookup, so the merge decision read one version's
    # rules for both writers — accepting a merge neither declared, or raising a
    # spurious conflict between two that did (issue #145, live QA).
    descriptors: Dict[Tuple[str, str], RecipeDescriptorV1] = {}
    for invocation in ordered:
        descriptors[
            (invocation.descriptor.recipe_id, invocation.descriptor.recipe_version)
        ] = invocation.descriptor
        contributions = _execute_deterministically(
            invocation.descriptor, active, invocation.validated_input
        )
        for index, contribution in enumerate(contributions):
            attributed.append(
                AttributedContributionV1(
                    invocation_id=invocation.invocation_id,
                    recipe_id=invocation.descriptor.recipe_id,
                    recipe_version=invocation.descriptor.recipe_version,
                    index=index,
                    contribution=contribution,
                )
            )

    composed = compose(attributed, descriptors)

    components = _resolve_components(composed, catalog)
    process_artifacts = _compile_processes(composed, components, connector_metadata, resolver)
    topology_plans = _plan_topologies(composed, topology_context)
    _evaluate_constraints(composed, components, active)

    return RecipeRunResultV1(
        composed=composed,
        components=tuple(components),
        process_artifacts=tuple(process_artifacts),
        topology_plans=tuple(topology_plans),
        provenance={
            "registry_revision": active.registry_revision,
            "recipes": [
                {
                    "recipe_id": inv.descriptor.recipe_id,
                    "recipe_version": inv.descriptor.recipe_version,
                    "entry_kind": inv.descriptor.entry_kind,
                    "implementation_sha256": inv.descriptor.provenance.implementation_sha256,
                    "invocation_id": inv.invocation_id,
                }
                for inv in ordered
            ],
        },
    )


def _resolve_components(
    composed: ComposedContributionsV1, catalog: MaterializationCatalog
) -> List[IntegrationComponentSpec]:
    """Slot -> real component, in contribution order, with headers verified."""
    resolved: List[IntegrationComponentSpec] = []
    for item in composed.component_slots:
        contribution = item.contribution
        resolved.append(
            catalog.resolve(
                contribution.materializer_slot,
                component_key=contribution.component_key,
                component_type=contribution.component_type,
                materialization_mode=contribution.materialization_mode,
            )
        )
    return resolved


def _compile_processes(
    composed: ComposedContributionsV1,
    components: Sequence[IntegrationComponentSpec],
    connector_metadata: Optional[Mapping[str, Tuple[Optional[str], Optional[str]]]],
    resolver,
) -> List[Tuple[str, Any]]:
    """The canonical chain, per assembled process. No exemption is reachable."""
    from ..compiler.process_ir.diagnostics import ProcessIRCompileError
    from ..compiler.process_ir.emitter_registry import emit_process
    from ..compiler.process_ir.pipeline import compile_process_ir_v1
    from ..models.process_ir import parse_process_ir_v1

    symbols = build_symbol_table(
        components, connector_metadata=connector_metadata, resolver=resolver
    )
    artifacts: List[Tuple[str, Any]] = []
    for process_key, root in composed.process_roots:
        try:
            reparsed = parse_process_ir_v1(root.model_dump(mode="json"))
            # validation_policy is NOT a parameter of run_recipes and is pinned
            # to None here. A legacy dialect's exemptions are unreachable from
            # the recipe path by construction.
            _cfg, plan = compile_process_ir_v1(reparsed, symbols, validation_policy=None)
            artifacts.append((process_key, emit_process(plan, symbols)))
        except ProcessIRCompileError as exc:
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"process:{process_key}",
                        cause_codes=tuple(d.code for d in exc.diagnostics),
                    ),
                )
            ) from None
        except Exception as exc:  # noqa: BLE001
            codes = tuple(
                getattr(d, "code", "") for d in getattr(exc, "diagnostics", ())
            )
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"process:{process_key}",
                        cause_codes=tuple(c for c in codes if c),
                    ),
                )
            ) from None
    return artifacts


def _plan_topologies(
    composed: ComposedContributionsV1, topology_context: Any
) -> List[Tuple[str, Any]]:
    """Parse and PLAN every assembled topology. Plan only — there is no apply."""
    if not composed.topologies:
        return []
    if topology_context is None:
        raise recipe_error(
            RECIPE_CONSTRAINT_FAILED,
            phase="validation",
            target="topology_context_missing",
        )

    from ..compiler.system_topology.pipeline import plan_system_topology
    from ..models.system_topology import (
        SystemTopologyValidationError,
        parse_system_topology_v1,
    )

    plans: List[Tuple[str, Any]] = []
    for topology_id, payload in composed.topologies:
        try:
            spec = parse_system_topology_v1(payload)
        except SystemTopologyValidationError as exc:
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"topology:{topology_id}",
                        cause_codes=tuple(d.code for d in exc.diagnostics),
                    ),
                )
            ) from None
        plan = plan_system_topology(spec, topology_context, "plan")
        if plan.blockers:
            raise RecipeError(
                (
                    recipe_diagnostic(
                        RECIPE_CONSTRAINT_FAILED,
                        phase="validation",
                        target=f"topology:{topology_id}",
                        cause_codes=tuple(b.code for b in plan.blockers),
                    ),
                )
            )
        plans.append((topology_id, plan))
    return plans


def _evaluate_constraints(
    composed: ComposedContributionsV1,
    components: Sequence[IntegrationComponentSpec],
    registry: RecipeRegistry,
) -> None:
    """Evaluate every declared requirement against the canonical artifacts.

    A constraint can only ADD an obligation. There is no path here by which one
    could satisfy itself, suppress a validator, or mark a violation safe — the
    artifacts were already produced by the canonical chain above, and this loop
    only reads them.
    """
    by_key = {component.key: component for component in components}
    process_keys = {key for key, _ in composed.process_roots}
    topologies = {topology_id: payload for topology_id, payload in composed.topologies}

    failures: List[str] = []
    for item in composed.constraints:
        requirement = item.contribution.requirement
        kind = requirement.kind
        if kind == "component":
            component = by_key.get(requirement.component_key)
            if component is None or component.type != requirement.component_type:
                failures.append("component")
        elif kind == "process":
            if requirement.process_key not in process_keys:
                failures.append("process")
        elif kind == "topology_object":
            payload = topologies.get(requirement.topology_id)
            if payload is None or not any(
                obj["key"] == requirement.object_key
                and obj["kind"] == requirement.object_kind
                for obj in payload["objects"]
            ):
                failures.append("topology_object")
        elif kind == "topology_relation":
            payload = topologies.get(requirement.topology_id)
            if payload is None or not any(
                rel["key"] == requirement.relation_key
                and rel["kind"] == requirement.relation_kind
                for rel in payload["relations"]
            ):
                failures.append("topology_relation")
        elif kind == "capability":
            from .contracts import RecipeCapabilityRequirementV1

            probe = RecipeCapabilityRequirementV1(
                authority=requirement.authority,
                subject=requirement.subject,
                required_state=requirement.required_state,
            )
            if not registry.capability_satisfied(probe):
                failures.append("capability")

    if failures:
        raise RecipeError(
            (
                recipe_diagnostic(
                    RECIPE_CONSTRAINT_FAILED,
                    phase="validation",
                    target="constraint_requirement",
                    cause_codes=tuple(f"requirement:{name}" for name in failures),
                ),
            )
        )


__all__ = [
    "RecipeRequestV1",
    "RecipeRunResultV1",
    "run_recipes",
]
