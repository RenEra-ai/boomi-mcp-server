"""Read-only authoring orchestration: plan, compile, apply-preflight (#146, M12.11).

Three entry points, two of which mutate nothing and one of which exists purely to
stop a mutation:

``plan_authoring_request_v1``
    Validate typed intent, resolve references read-only, evaluate topology, and
    return the ``IntegrationSpecV1`` ComponentPlan preview plus gaps and decisions.
``compile_authoring_request_v1``
    Re-run the plan path, then compile every authored process through the ONE
    canonical chain and publish deterministic artifact fingerprints.
``preflight_typed_apply_v1``
    Recompute the compile in the ACTIVE profile and compare the caller's binding.
    Returns a compiled bundle only when every check passes.

**Compile re-runs plan; it never trusts a client-supplied plan result.** A plan
result is data the caller holds, and data the caller holds is data the caller can
edit. The only plan a compile may act on is the one it just recomputed itself.

**Apply re-parses from the raw payload.** ``ProcessIRV1`` is strict but not
frozen, and "the payload changed after it was checked" is the failure that
matters when a mutation is downstream. The preflight takes the raw request and
re-parses it rather than accepting an object handed down from the plan call.

**Zero mutation is structural, not documented.** Neither read-only function
imports or calls a create/update/execute/deploy helper. The typed apply branch in
``integration_builder`` takes the compiled bundle as a REQUIRED argument, so a
code path that reaches materialization without a passed preflight does not exist.

**What ``integration_spec`` intent can and cannot do.** A ProcessIR root is not
derivable from every legacy ``process_kind``: the #139 adapters cover three
migrated dialects, and the rest stay on the legacy renderer. So this intent plans
and previews the component plan it was given, and reports an explicit
``CapabilityGapV1`` for each process whose IR it cannot derive — honest and
non-blocking. It does NOT invent a second adapter, and it does not silently
reinterpret a legacy request as something it is not.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import (
    AUTHORING_APPLY_VALIDATION_REQUIRED,
    AUTHORING_CAPABILITY_REVISION_MISMATCH,
    AUTHORING_COMPILE_BLOCKED,
    AUTHORING_LIVE_DEPLOYMENT_DRIFT,
    AUTHORING_PLAN_STALE,
    AUTHORING_REQUIRED_DECISION_MISSING,
)
from ..models.authoring_workflow import (
    ArtifactFingerprintV1,
    AuthoringCompileResultV1,
    AuthoringDiagnosticV1,
    AuthoringPlanResultV1,
    AuthoringRequestV1,
    AuthoringRevisionBindingV1,
    CapabilityGapV1,
    ComponentDependencyEdgeV1,
    LiveDeploymentComparisonV1,
    ProcessCfgSummaryV1,
    RequiredDecisionV1,
    ResolvedReferenceSummaryV1,
    TopologyParticipantV1,
    TopologyRelationSummaryV1,
    ValidationReportSummaryV1,
    sort_authoring_diagnostics,
    sort_by_key,
)
from ..models.integration_models import IntegrationComponentSpec, IntegrationSpecV1
from .contract import compare_capability_revision, get_authoring_revisions
from .revisions import (
    account_scope_fingerprint,
    artifact_fingerprint,
    compile_fingerprint,
    plan_fingerprint,
    semantic_fingerprint,
    sha256_fingerprint,
)

_REF_PREFIX = "$ref:"


class AuthoringWorkflowError(Exception):
    """A blocking authoring-surface failure, carrying ordered diagnostics.

    Raised only where continuing would mean reporting a result the server cannot
    stand behind. Everything non-blocking is a ``CapabilityGapV1`` or a warning
    in the result, not an exception.
    """

    def __init__(
        self,
        code: str,
        diagnostics: Tuple[AuthoringDiagnosticV1, ...],
        *,
        partial: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.diagnostics = sort_authoring_diagnostics(diagnostics)
        self.partial = partial or {}


def _diag(
    code: str,
    severity: str,
    *,
    message: str,
    path: str = "",
    subject_kind: str = "",
    subject_id: str = "",
    remediation: str = "",
    cause_codes: Tuple[str, ...] = (),
) -> AuthoringDiagnosticV1:
    return AuthoringDiagnosticV1(
        code=code,
        severity=severity,
        path=path,
        subject_kind=subject_kind,
        subject_id=subject_id,
        message=message,
        remediation=remediation,
        cause_codes=tuple(sorted(set(cause_codes))),
    )


# ---------------------------------------------------------------------------
# normalization: intent -> (component plan, process roots, topology, gaps)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _NormalizedIntent:
    integration_spec: IntegrationSpecV1
    #: ``(component_key, ProcessIRV1)`` sorted by key. Sorted rather than
    #: insertion-ordered because the semantic hash is computed over it, and a
    #: hash that depends on mapping insertion order is not a fingerprint.
    process_roots: Tuple[Tuple[str, Any], ...]
    gaps: Tuple[CapabilityGapV1, ...]
    connector_metadata: Mapping[str, Tuple[Optional[str], Optional[str]]]


def _connector_metadata_from_components(
    components: Sequence[IntegrationComponentSpec],
) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """``component key -> (connector_type, action_type)``, read off the PLAN.

    ADR-001 §6 and ``recipes.materialization.build_symbol_table`` agree that no
    connector-action component declares its own connection: the family is a fact
    of the component plan the compiler receives, never something the IR authors.
    So it is derived here, from each component's own config, and the IR is never
    consulted for it.
    """
    metadata: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for component in components:
        config = component.config or {}
        connector_type = config.get("connector_type")
        action_type = config.get("action_type")
        if connector_type or action_type:
            metadata[component.key] = (
                str(connector_type) if connector_type else None,
                str(action_type) if action_type else None,
            )
    return metadata


#: Why an intent that COMPILES a process cannot be applied.
#:
#: Boomi processes are created by the legacy builders, which emit XML from
#: ``config.process_kind``. Nothing on a production path materializes a ProcessIR
#: root: the compiler stops at the emission plan, and promoting its emitter to a
#: production component writer is an ADR-001 §9 byte-parity cutover with its own
#: issue.
#:
#: The rule keys on the COMPILED ARTIFACT, not on the intent kind: if compilation
#: fingerprinted a ProcessIR root, apply must materialize that root. It applies
#: equally to a direct ``process_ir`` intent and to a ``recipe`` intent whose
#: composed roots were compiled — refusing one while permitting the other made
#: the safety argument incoherent, since both certify one representation and
#: build another.
#:
#: A ``process_kind`` on the component would not rescue either: the builder would
#: emit from the component config while the binding attested to the emission
#: plan, so the compile hash would certify an artifact that was never created. A
#: silent divergence is worse than a missing capability, and "the binding means
#: what it says" is the whole point of this milestone.
#:
#: ``integration_spec`` is unaffected — it produces no process roots, so its
#: binding never claims a process artifact.
MATERIALIZATION_CAPABILITY = "authoring.typed_apply.process_materialization"


def _materialization_gaps(
    request: AuthoringRequestV1,
    spec: IntegrationSpecV1,
    process_roots: Tuple[Tuple[str, Any], ...] = (),
) -> Tuple[CapabilityGapV1, ...]:
    """Any intent whose COMPILED process is not what apply would BUILD.

    Keyed on the compiled artifact, not on the intent kind. The rule is one
    sentence: if compilation fingerprinted a ProcessIR root, apply must
    materialize THAT — and no production path does, because process XML is
    emitted from the component plan's ``process_kind`` by the legacy builders.

    An earlier version keyed on ``intent_kind == "process_ir"`` alone, which was
    an inconsistency rather than a policy: a typed RECIPE intent also has its
    composed ProcessIR roots compiled and fingerprinted, while apply builds from
    the component configuration the recipe emitted alongside them. That is the
    identical divergence — one representation certified, another built — and
    refusing it for one intent while permitting it for the other made the safety
    argument incoherent.

    ``integration_spec`` is unaffected: it produces no process roots, so the
    binding never claims a process artifact and apply is the legacy behaviour
    plus a binding over the component plan.
    """
    if not process_roots:
        return ()
    kind = request.intent.intent_kind
    return (
        CapabilityGapV1(
            capability_id=MATERIALIZATION_CAPABILITY,
            state="unsupported",
            path=f"/intent/{kind}",
            reason_code="PROCESS_KIND_REQUIRED",
            detail=(
                "This intent compiles a ProcessIR root, so it can be planned and "
                "compiled — both read-only — but not applied: process "
                "materialization emits XML from the component plan, so applying "
                "it would create an artifact the compile hash does not describe. "
                "Use build_from_archetype, whose adapter proves byte parity "
                "against the legacy renderer, to produce a materializable "
                "component plan."
            ),
        ),
    )


def _normalize_intent(request: AuthoringRequestV1) -> _NormalizedIntent:
    intent = request.intent
    kind = intent.intent_kind

    if kind == "integration_spec":
        spec = intent.integration_spec
        gaps = tuple(
            CapabilityGapV1(
                capability_id=(
                    "authoring.integration_spec_intent."
                    + str((component.config or {}).get("process_kind") or "unknown")
                ),
                state="unsupported",
                path=f"/intent/integration_spec/components/{component.key}",
                reason_code="PROCESS_IR_ROOT_NOT_DERIVABLE",
                detail=(
                    "This process is planned and previewed from the component "
                    "plan, but its ProcessIR root is not derivable, so canonical "
                    "compilation produces no artifact fingerprint for it. Author "
                    "the process with intent_kind='process_ir' to compile it."
                ),
            )
            for component in spec.components
            if component.type == "process"
        )
        return _NormalizedIntent(
            integration_spec=spec,
            process_roots=(),
            gaps=gaps,
            connector_metadata=_connector_metadata_from_components(spec.components),
        )

    if kind == "process_ir":
        spec = IntegrationSpecV1(
            name=intent.integration_name,
            components=list(intent.components),
        )
        return _NormalizedIntent(
            integration_spec=spec,
            process_roots=((intent.component_key, intent.process_ir),),
            gaps=(),
            connector_metadata=_connector_metadata_from_components(intent.components),
        )

    # kind == "recipe"
    return _normalize_recipe_intent(intent)


def _normalize_recipe_intent(intent) -> _NormalizedIntent:
    """Run the registered recipes and take their assembled output.

    Composition is NOT re-implemented here: ``run_recipes`` owns descriptor
    resolution, ordering, patch composition, the input gate, and the canonical
    validation gate. This function calls it and reads its result.
    """
    from ..recipes.engine import RecipeRequestV1, run_recipes
    from ..recipes.errors import RecipeError
    from ..recipes.materialization import MaterializationCatalog

    catalog_entries = {
        component.key: component for component in intent.base_components
    }
    requests = [
        RecipeRequestV1(
            recipe_id=invocation.recipe_id,
            invocation_id=invocation.invocation_id,
            raw_input=dict(invocation.raw_input),
            recipe_version=invocation.recipe_version,
        )
        for invocation in intent.invocations
    ]
    connector_metadata = _connector_metadata_from_components(intent.base_components)
    try:
        result = run_recipes(
            requests,
            catalog=MaterializationCatalog(catalog_entries),
            connector_metadata=connector_metadata,
        )
    except RecipeError as exc:
        # The recipe layer's own codes are carried VALUE-FREE as causatives; the
        # authoring surface reports which phase blocked, never re-diagnoses the
        # recipe layer's domain (ADR-001 §7).
        raise AuthoringWorkflowError(
            AUTHORING_COMPILE_BLOCKED,
            tuple(
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "error",
                    message="The recipe layer refused to assemble this intent.",
                    subject_kind="recipe",
                    subject_id=getattr(diagnostic, "target", "") or "",
                    remediation=(
                        "Fix the recipe input or invocation and re-plan. See "
                        "cause_codes for the recipe layer's own diagnosis."
                    ),
                    cause_codes=tuple(
                        getattr(diagnostic, "cause_codes", ()) or ()
                    )
                    + ((getattr(diagnostic, "code", "") or "",)),
                )
                for diagnostic in (getattr(exc, "diagnostics", ()) or ())
            )
            or (
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "error",
                    message="The recipe layer refused to assemble this intent.",
                    subject_kind="recipe",
                ),
            ),
        ) from None

    components = list(result.components)
    spec = IntegrationSpecV1(name=intent.integration_name, components=components)
    roots = tuple(sorted(result.composed.process_roots, key=lambda pair: pair[0]))
    return _NormalizedIntent(
        integration_spec=spec,
        process_roots=roots,
        gaps=(),
        connector_metadata=_connector_metadata_from_components(components),
    )


# ---------------------------------------------------------------------------
# read-only evidence gathering
# ---------------------------------------------------------------------------


def build_integration_spec_preview(normalized: _NormalizedIntent) -> IntegrationSpecV1:
    """The ComponentPlan preview — explicitly an ``IntegrationSpecV1``.

    Named a preview and not a plan: it is what apply WOULD materialize, and
    calling it the plan invites reading it as something already true.
    """
    return normalized.integration_spec


def build_component_dependencies(
    spec: IntegrationSpecV1,
) -> Tuple[ComponentDependencyEdgeV1, ...]:
    """ComponentPlan materialization edges — not CFG edges, not topology relations."""
    edges = [
        ComponentDependencyEdgeV1(component_key=component.key, depends_on=dependency)
        for component in spec.components
        for dependency in (component.depends_on or ())
    ]
    return sort_by_key(edges)


def build_pipeline_stages(spec: IntegrationSpecV1) -> Tuple[str, ...]:
    """The inert ``PipelineSpec`` stage KEYS (ADR-001 §5), if one was authored.

    Echoed, never executed: a v1.0 spec's top-level pipeline drives nothing, and
    surfacing it under its own name keeps that visible instead of letting a
    reader assume it is the process.

    ``StageSpec`` identifies itself with ``key`` and has no ``name`` field. An
    earlier version read ``name`` behind a ``getattr`` default, so every authored
    pipeline summarized as a list of empty strings — a defensive default on a
    model whose fields we control, hiding the error instead of surfacing it.
    """
    pipeline = getattr(spec, "pipeline", None)
    if pipeline is None:
        return ()
    return tuple(str(stage.key) for stage in (pipeline.stages or ()))


#: Fields every topology relation carries; everything else names a participant.
_TOPOLOGY_RELATION_META_FIELDS = frozenset({"kind", "key"})


def build_topology_relations(
    topology_spec,
) -> Tuple[TopologyRelationSummaryV1, ...]:
    """Summarize topology relations from the fields they ACTUALLY declare.

    Each variant carries ``kind`` and ``key`` plus role-specific object
    references — ``caller_process``/``callee_process`` for a process call,
    ``deployment_unit``/``process``/``environment`` for a deployment binding, and
    so on. Participants are therefore derived from the model's own field set
    rather than squeezed into a source/target pair, which is both wrong for the
    three-role variants and, as written before, wrong for all of them: no variant
    defines ``relation_kind``, ``source_key`` or ``target_key``, so every
    relation summarized as empty strings behind a ``getattr`` default.
    """
    if topology_spec is None:
        return ()
    summaries = []
    for relation in topology_spec.relations or ():
        participants = tuple(
            sorted(
                (
                    TopologyParticipantV1(
                        role=field,
                        ref=str(getattr(relation, field, "") or ""),
                    )
                    for field in type(relation).model_fields
                    if field not in _TOPOLOGY_RELATION_META_FIELDS
                ),
                key=lambda participant: participant.sort_key,
            )
        )
        summaries.append(
            TopologyRelationSummaryV1(
                relation_kind=str(relation.kind),
                relation_key=str(getattr(relation, "key", "") or ""),
                participants=participants,
            )
        )
    return sort_by_key(summaries)


def _iter_authored_refs(value: Any) -> List[str]:
    """Every ``$ref:KEY`` token the authored intent actually uses.

    Walks the payload rather than listing the component plan's own keys. Those
    are two different questions, and live QA showed the difference matters: a
    dangling ``$ref`` is the ONE reference a caller needs to see, and it is
    exactly the one a component-key listing omits (issue #146 QA, bug #410).
    """
    found: List[str] = []
    if isinstance(value, str):
        if value.startswith(_REF_PREFIX):
            found.append(value)
    elif isinstance(value, Mapping):
        for item in value.values():
            found.extend(_iter_authored_refs(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            found.extend(_iter_authored_refs(item))
    return found


def _legacy_plan_echo(
    normalized: _NormalizedIntent,
    request: AuthoringRequestV1,
    boomi_client: Any,
) -> Optional[Dict[str, Any]]:
    """Run the LEGACY component-plan lint over the typed component plan.

    Reused rather than reimplemented, and that reuse is load-bearing twice over
    (both found by live QA, issue #146):

    * The legacy planner REDACTS secrets from the spec it echoes. A typed path
      that echoed the caller's ``integration_spec`` directly returned a password
      in plaintext that the same wrapper redacted down the legacy root.
    * The legacy planner is where duplicate-connection, base-URL, folder and
      name lints live. A typed plan that skipped them did not merely omit
      warnings — it affirmatively reported ``warnings: []`` for a spec the
      legacy plan flagged five times, which is worse than saying nothing.

    Returns ``None`` when there is no client to plan against; the caller then
    reports the lint as unavailable rather than as clean.
    """
    if boomi_client is None:
        return None
    try:
        from ..categories.integration_builder import _build_plan

        result = _build_plan(
            boomi_client,
            {
                "integration_spec": normalized.integration_spec.model_dump(mode="json"),
                "conflict_policy": request.intent.conflict_policy,
            },
        )
    except Exception:  # noqa: BLE001 — the lint is evidence, never the gate
        return None

    # A FAILED legacy plan is not a lint that ran clean. It returns no
    # `integration_spec`, so treating it as success left `spec_preview` as the
    # RAW request — and the path that reaches it includes
    # `PLAINTEXT_SECRET_REJECTED` on a top-level pipeline, meaning the typed
    # response would have echoed the very value the legacy planner refused
    # value-free, while reporting success. Returned as a failure so the caller
    # sees the refusal instead.
    if not result.get("_success"):
        raise AuthoringWorkflowError(
            AUTHORING_COMPILE_BLOCKED,
            (
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "error",
                    message=(
                        "The component-plan lint rejected this intent; its "
                        "diagnosis is carried value-free."
                    ),
                    subject_kind="component_plan",
                    remediation=(
                        "Fix the component plan and re-plan. See cause_codes for "
                        "the legacy planner's own code."
                    ),
                    cause_codes=(
                        str(result.get("error_code") or "LEGACY_PLAN_REJECTED"),
                    ),
                ),
            ),
        )
    return result


def _resolve_component_by_id(boomi_client: Any, component_id: str):
    """Read-only metadata for one component id, or ``None``.

    Exists so a component carrying an explicit ``component_id`` still yields a
    ``version_marker``. Without it, the staleness guarantee held only for
    components resolved by NAME.
    """
    from boomi.models import (
        ComponentMetadataQueryConfig,
        ComponentMetadataQueryConfigQueryFilter,
        ComponentMetadataSimpleExpression,
        ComponentMetadataSimpleExpressionOperator,
        ComponentMetadataSimpleExpressionProperty,
    )

    from ..categories.integration_builder import paginate_metadata

    expression = ComponentMetadataSimpleExpression(
        operator=ComponentMetadataSimpleExpressionOperator.EQUALS,
        property=ComponentMetadataSimpleExpressionProperty.COMPONENTID,
        argument=[component_id],
    )
    query_filter = ComponentMetadataQueryConfigQueryFilter(expression=expression)
    matches = paginate_metadata(
        boomi_client, ComponentMetadataQueryConfig(query_filter=query_filter),
        show_all=False,
    )
    return matches[0] if matches else None


def build_resolved_reference_summary(
    spec: IntegrationSpecV1,
    *,
    boomi_client: Any = None,
    authored_refs: Sequence[str] = (),
) -> Tuple[ResolvedReferenceSummaryV1, ...]:
    """Read-only reference evidence, sanitized.

    Two sources, in order of authority: a component the plan itself declares
    (resolved locally, no call), and — only when a client is supplied — an
    existing live component matched by type+name through the SAME read-only
    metadata query ``_build_plan`` already uses.

    ``version_marker`` is the staleness evidence. Without it a component edited
    between plan and apply would leave every hash unchanged, and the binding
    would certify semantics that no longer hold.

    A lookup failure leaves ``version_marker`` unset rather than raising, and
    does NOT flip ``resolved``: whether a reference resolves is a property of the
    component PLAN, which is known locally, so a discovery outage must not
    reclassify a valid in-plan reference as dangling. Planning must still return
    the caller's gaps, and an outage is not a reason to refuse to describe the
    plan — but it is also not evidence that a reference is broken.
    """
    summaries: List[ResolvedReferenceSummaryV1] = []
    declared = {component.key: component for component in spec.components}

    for key, component in sorted(declared.items()):
        component_id: Optional[str] = component.component_id
        version_marker: Optional[str] = None
        # A `$ref` naming a component THIS PLAN declares is resolved by the
        # compiler's symbol table whether or not that component exists yet.
        # Marking it unresolved made a valid in-plan reference to a
        # to-be-created component indistinguishable from a genuinely dangling
        # one — which is exactly the distinction this field exists to draw.
        resolved = True

        if boomi_client is not None:
            try:
                from ..categories.integration_builder import (
                    _resolve_existing_components,
                )

                match = None
                if component_id:
                    # Resolve by ID too, not only by name. Skipping the lookup
                    # for an explicit id left `version_marker` empty, so editing
                    # that component between compile and apply moved no hash and
                    # a stale binding passed the gate — the exact guarantee the
                    # marker is documented to provide.
                    match = _resolve_component_by_id(boomi_client, component_id)
                elif component.name:
                    matches = _resolve_existing_components(boomi_client, component)
                    match = matches[0] if matches else None

                if match:
                    component_id = match.get("component_id") or component_id
                    version_marker = str(
                        match.get("version") or match.get("modified_date") or ""
                    ) or None
            except Exception:  # noqa: BLE001 — advisory evidence, never fatal
                version_marker = None

        summaries.append(
            ResolvedReferenceSummaryV1(
                ref=f"{_REF_PREFIX}{key}",
                component_type=component.type,
                resolved=resolved,
                component_id=component_id,
                version_marker=version_marker,
            )
        )

    # Every ``$ref`` the intent actually USES that the component plan does not
    # declare. Listing only declared keys omitted the dangling reference — the
    # one an agent most needs to see (issue #146 QA, bug #410).
    declared_refs = {f"{_REF_PREFIX}{key}" for key in declared}
    for ref in sorted(set(authored_refs) - declared_refs):
        summaries.append(
            ResolvedReferenceSummaryV1(
                ref=ref,
                component_type="",
                resolved=False,
                component_id=None,
                version_marker=None,
            )
        )
    return sort_by_key(summaries)


def _validate_processes(
    normalized: _NormalizedIntent,
) -> Tuple[ValidationReportSummaryV1, Tuple[AuthoringDiagnosticV1, ...], Any]:
    """Run the unified #143 semantic validator over every authored process.

    Uses ``validate_process_ir``, which REPORTS and does not raise on a bad
    payload — planning must be able to hand a caller everything wrong with their
    intent at once, not the first thing that stopped it.
    """
    from ..compiler.process_ir.semantic_validation.pipeline import validate_process_ir
    from ..recipes.materialization import build_symbol_table

    symbols = build_symbol_table(
        list(normalized.integration_spec.components),
        connector_metadata=normalized.connector_metadata,
    )

    errors = 0
    warnings = 0
    advisories = 0
    codes: List[str] = []
    diagnostics: List[AuthoringDiagnosticV1] = []

    for component_key, ir in normalized.process_roots:
        report = validate_process_ir(ir, symbols)
        errors += len(report.errors)
        warnings += len(report.warnings)
        advisories += len(report.advisories)
        for finding in report.errors:
            codes.append(finding.code)
            diagnostics.append(
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "error",
                    message="Semantic validation rejected this process.",
                    path=finding.path,
                    subject_kind="process",
                    subject_id=component_key,
                    remediation="Fix the reported node and re-plan.",
                    cause_codes=(finding.code,),
                )
            )
        for finding in report.warnings:
            codes.append(finding.code)
            diagnostics.append(
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "warning",
                    message="Semantic validation warned about this process.",
                    path=finding.path,
                    subject_kind="process",
                    subject_id=component_key,
                    cause_codes=(finding.code,),
                )
            )

    summary = ValidationReportSummaryV1(
        is_valid=errors == 0,
        error_count=errors,
        warning_count=warnings,
        advisory_count=advisories,
        codes=tuple(sorted(set(codes))),
    )
    return summary, tuple(diagnostics), symbols


def _validate_topology(
    request: AuthoringRequestV1, normalized: _NormalizedIntent, profile: str
) -> Tuple[AuthoringDiagnosticV1, ...]:
    """Validate the topology adjunct. Planning only — there is no apply path."""
    if request.topology_spec is None:
        return ()
    try:
        from ..compiler.system_topology.context import (
            ComponentPlanSymbolV1,
            TopologyResolutionContextV1,
        )
        from ..compiler.system_topology.pipeline import validate_system_topology

        symbols = tuple(
            ComponentPlanSymbolV1(
                component_key=component.key, component_type=component.type
            )
            for component in normalized.integration_spec.components
        )
        context = TopologyResolutionContextV1(
            profile=profile, component_plan_symbols=symbols
        )
        report = validate_system_topology(request.topology_spec, context)
    except Exception as exc:  # noqa: BLE001 — a planner defect must not be silent
        return (
            _diag(
                AUTHORING_COMPILE_BLOCKED,
                "error",
                message="Topology validation could not run for this intent.",
                subject_kind="topology",
                remediation="Remove topology_spec or correct it, then re-plan.",
                cause_codes=(type(exc).__name__,),
            ),
        )

    diagnostics = []
    for finding in getattr(report, "errors", ()) or ():
        diagnostics.append(
            _diag(
                AUTHORING_COMPILE_BLOCKED,
                "error",
                message="Topology validation rejected this relation or object.",
                path=getattr(finding, "path", "") or "",
                subject_kind="topology",
                remediation="Fix the topology relation and re-plan.",
                cause_codes=(getattr(finding, "code", "") or "",),
            )
        )
    for finding in getattr(report, "warnings", ()) or ():
        diagnostics.append(
            _diag(
                AUTHORING_COMPILE_BLOCKED,
                "warning",
                message="Topology validation warned about this relation or object.",
                path=getattr(finding, "path", "") or "",
                subject_kind="topology",
                cause_codes=(getattr(finding, "code", "") or "",),
            )
        )
    return tuple(diagnostics)


def _evaluate_decisions(
    request: AuthoringRequestV1, normalized: _NormalizedIntent
) -> Tuple[Tuple[RequiredDecisionV1, ...], Tuple[AuthoringDiagnosticV1, ...]]:
    """Decisions the server refuses to make for the caller.

    No decision FAMILY is raised by this slice: every fork the typed path can
    currently reach is either settled by the intent's own ``conflict_policy`` or
    reported as a capability gap. The machinery is here — and tested — because
    the contract publishes ``required_decisions`` on every response, and a field
    that is always empty because nothing can populate it is indistinguishable
    from one that is empty because nothing is wrong.

    What IS enforced today is the inverse direction, which is the one that
    silently corrupts a binding: a resolution naming a decision this plan never
    raised means the caller is answering a question from a DIFFERENT plan.
    """
    resolutions = {
        resolution.decision_id: resolution.option_id
        for resolution in request.decisions
    }
    decisions: List[RequiredDecisionV1] = []
    diagnostics: List[AuthoringDiagnosticV1] = []

    raised = {decision.decision_id for decision in decisions}
    for decision_id in sorted(set(resolutions) - raised):
        diagnostics.append(
            _diag(
                AUTHORING_REQUIRED_DECISION_MISSING,
                "error",
                message="A decision resolution names a decision this plan did not raise.",
                subject_kind="decision",
                subject_id=decision_id,
                remediation="Re-plan and answer only the decisions it returns.",
            )
        )

    for decision in decisions:
        if not decision.resolved:
            diagnostics.append(
                _diag(
                    AUTHORING_REQUIRED_DECISION_MISSING,
                    "error",
                    message="A required decision has no resolution.",
                    path=decision.path,
                    subject_kind="decision",
                    subject_id=decision.decision_id,
                    remediation="Supply decisions[].option_id and re-plan.",
                )
            )

    return sort_by_key(decisions), tuple(diagnostics)


def _binding(
    *,
    profile: str,
    account_id: Optional[str],
    semantic_hash: Optional[str] = None,
    plan_hash: Optional[str] = None,
    compile_hash: Optional[str] = None,
) -> AuthoringRevisionBindingV1:
    revisions = get_authoring_revisions()
    return AuthoringRevisionBindingV1(
        schema_revision=revisions["schema_revision"],
        capability_revision=revisions["capability_revision"],
        compiler_revision=revisions["compiler_revision"],
        account_scope_hash=account_scope_fingerprint(profile, account_id),
        semantic_hash=semantic_hash,
        plan_hash=plan_hash,
        compile_hash=compile_hash,
    )


def _check_capability_revision(
    request: AuthoringRequestV1,
) -> Tuple[AuthoringDiagnosticV1, ...]:
    if request.expected_capability_revision is None:
        return ()
    comparison = compare_capability_revision(request.expected_capability_revision)
    if comparison["status"] == "match":
        return ()
    return (
        _diag(
            AUTHORING_CAPABILITY_REVISION_MISMATCH,
            "error",
            message=(
                "This server's authoring capability revision differs from the "
                "one this request was built against."
            ),
            subject_kind="capability_revision",
            remediation=comparison.get("remediation", ""),
        ),
    )


def _normalized_payload(normalized: _NormalizedIntent, request: AuthoringRequestV1):
    """The canonical, secret-free semantic payload the semantic hash covers.

    ``intent_kind`` and ``conflict_policy`` are hashed EXPLICITLY, from the
    request's own intent, not inferred from the normalized component plan.

    Live QA proved why: ``conflict_policy`` lives on the intent and never
    reaches the normalized spec, so hashing only the projection produced an
    identical ``compile_hash`` for ``reuse`` / ``clone`` / ``fail``. A caller
    could compile under ``fail`` and apply under ``clone`` with that binding —
    and it created an extra live component (issue #146 QA, bug #403). A binding
    must cover every input that changes what apply DOES, not only what it builds.
    """
    from ..models.process_ir import canonical_process_ir_json

    return {
        "intent_kind": request.intent.intent_kind,
        "conflict_policy": request.intent.conflict_policy,
        "integration_spec": normalized.integration_spec.model_dump(mode="json"),
        "process_roots": [
            {"component_key": key, "process_ir": canonical_process_ir_json(ir)}
            for key, ir in normalized.process_roots
        ],
        "topology_spec": (
            request.topology_spec.model_dump(mode="json")
            if request.topology_spec is not None
            else None
        ),
        "decisions": sorted(
            (
                {"decision_id": d.decision_id, "option_id": d.option_id}
                for d in request.decisions
            ),
            key=lambda entry: (entry["decision_id"], entry["option_id"]),
        ),
    }


# ---------------------------------------------------------------------------
# phase 1 — read-only semantic plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _PlanInternals:
    """Everything compile needs that the public plan result does not publish."""

    normalized: _NormalizedIntent
    symbols: Any
    semantic_hash: str
    plan_hash: str


def plan_authoring_request_v1(
    request: AuthoringRequestV1,
    *,
    boomi_client: Any = None,
    profile: str,
    account_id: Optional[str] = None,
) -> Tuple[AuthoringPlanResultV1, _PlanInternals]:
    """Validate typed intent and preview its ComponentPlan. Mutates nothing."""
    capability_diagnostics = _check_capability_revision(request)
    if capability_diagnostics:
        raise AuthoringWorkflowError(
            AUTHORING_CAPABILITY_REVISION_MISMATCH, capability_diagnostics
        )

    normalized = _normalize_intent(request)
    validation, validation_diagnostics, symbols = _validate_processes(normalized)
    topology_diagnostics = _validate_topology(request, normalized, profile)
    decisions, decision_diagnostics = _evaluate_decisions(request, normalized)

    all_diagnostics = (
        validation_diagnostics + topology_diagnostics + decision_diagnostics
    )
    errors = sort_authoring_diagnostics(
        tuple(d for d in all_diagnostics if d.severity == "error")
    )
    warnings = sort_authoring_diagnostics(
        tuple(d for d in all_diagnostics if d.severity != "error")
    )

    spec_preview = build_integration_spec_preview(normalized)

    # The LEGACY component-plan lint, reused. It supplies the redacted spec echo
    # and the duplicate-connection / base-URL / folder / name warnings that a
    # reimplementation silently lacked (issue #146 QA, bugs #401 and #402).
    legacy = _legacy_plan_echo(normalized, request, boomi_client)
    legacy_warnings: Tuple[AuthoringDiagnosticV1, ...] = ()
    if legacy is not None:
        # A step the legacy planner marked `error_*` CANNOT execute — that is
        # the planner's own vocabulary, and `_apply_plan` refuses on the same
        # prefix. Surfaced so compile stops issuing a binding whose apply is
        # guaranteed to fail while reporting `is_valid: true` and no warnings.
        #
        # BLOCKING only when apply is actually reachable. A direct ProcessIR
        # intent is plan/compile-only by design, and its component plan exists to
        # resolve `$ref` symbols rather than to be built — so blocking compile on
        # a materialization lint would make the one capability this issue adds
        # (compile a ProcessIR, get its artifact fingerprints) unusable unless
        # the caller also supplied a fully materializable component plan they
        # never intended to apply. There the finding is real but advisory.
        blocks_apply = request.intent.intent_kind != "process_ir"
        unexecutable = tuple(
            _diag(
                AUTHORING_COMPILE_BLOCKED,
                "error" if blocks_apply else "warning",
                message=(
                    "The component-plan lint marked this step unexecutable; "
                    "apply would refuse it."
                    if blocks_apply
                    else "The component-plan lint marked this step unexecutable. "
                    "This intent is plan/compile-only, so nothing would be built "
                    "from it either way."
                ),
                path=f"/components/{step.get('key', '')}",
                subject_kind="component",
                subject_id=str(step.get("key", "")),
                remediation=(
                    "Resolve the component collision, or re-plan with a "
                    "different conflict_policy."
                ),
                cause_codes=(str(step.get("planned_action", "")),),
            )
            for step in (legacy.get("steps") or ())
            if str(step.get("planned_action", "")).startswith("error_")
        )
        if blocks_apply:
            errors = sort_authoring_diagnostics(errors + unexecutable)
        # The redacted echo REPLACES the caller's spec. This is the single line
        # that stops a plaintext password from riding back out in the preview.
        if isinstance(legacy.get("integration_spec"), dict):
            spec_preview = IntegrationSpecV1(**legacy["integration_spec"])
        # ACCUMULATED, not reassigned: the advisory unexecutable-step findings
        # above must survive alongside the planner's own warning strings.
        legacy_warnings = (() if blocks_apply else unexecutable) + tuple(
            _diag(
                AUTHORING_COMPILE_BLOCKED,
                "warning",
                message=str(warning),
                subject_kind="component_plan",
                remediation="Review the component-plan lint before applying.",
                cause_codes=("LEGACY_PLAN_WARNING",),
            )
            for warning in (legacy.get("warnings") or ())
        )
    else:
        # Not silence — an explicit statement that the lint did not run. Reporting
        # zero warnings when nothing looked is the false negative this fixes.
        legacy_warnings = (
            _diag(
                AUTHORING_COMPILE_BLOCKED,
                "advisory",
                message=(
                    "The component-plan lint did not run for this request, so "
                    "its warnings are unknown rather than absent."
                ),
                subject_kind="component_plan",
                remediation="Re-plan with an authenticated profile.",
            ),
        )
    warnings = sort_authoring_diagnostics(warnings + legacy_warnings)

    # The summary must describe THIS RESULT's diagnostics, not just the semantic
    # validator's slice of them. Reporting `warning_count: 0` beside a `warnings`
    # list holding five entries is the same "positively asserts absence" defect
    # the lint fix above was filed for, one field over (issue #146 QA, bug #412).
    validation = ValidationReportSummaryV1(
        is_valid=not errors,
        error_count=len(errors),
        warning_count=sum(1 for d in warnings if d.severity == "warning"),
        advisory_count=sum(1 for d in warnings if d.severity == "advisory"),
        codes=tuple(
            sorted(
                {code for d in errors + warnings for code in d.cause_codes}
                | set(validation.codes)
            )
        ),
    )

    references = build_resolved_reference_summary(
        spec_preview,
        boomi_client=boomi_client,
        authored_refs=_iter_authored_refs(
            [ir.model_dump(mode="json") for _key, ir in normalized.process_roots]
            + [normalized.integration_spec.model_dump(mode="json")]
        ),
    )
    # Surfaced at PLAN time, not first discovered at apply: a caller must be able
    # to learn that this intent is plan/compile-only before spending a compile.
    gaps = sort_by_key(
        normalized.gaps
        + _materialization_gaps(request, spec_preview, normalized.process_roots)
    )

    semantic_hash = semantic_fingerprint(_normalized_payload(normalized, request))
    binding_without_plan = _binding(
        profile=profile, account_id=account_id, semantic_hash=semantic_hash
    )
    plan_hash = plan_fingerprint(
        semantic_hash=semantic_hash,
        revision_binding=binding_without_plan.model_dump(mode="json"),
        resolved_references=[r.model_dump(mode="json") for r in references],
        validation_report=validation.model_dump(mode="json"),
        capability_gaps=[g.model_dump(mode="json") for g in gaps],
        required_decisions=[d.model_dump(mode="json") for d in decisions],
        integration_spec_preview=spec_preview.model_dump(mode="json"),
    )

    if request.expected_plan_hash is not None and request.expected_plan_hash != plan_hash:
        raise AuthoringWorkflowError(
            AUTHORING_PLAN_STALE,
            (
                _diag(
                    AUTHORING_PLAN_STALE,
                    "error",
                    message=(
                        "The recomputed plan differs from the one this request "
                        "was bound to."
                    ),
                    subject_kind="plan_hash",
                    remediation="Re-plan, then recompile against the new plan hash.",
                ),
            ),
        )

    result = AuthoringPlanResultV1(
        revision_binding=_binding(
            profile=profile,
            account_id=account_id,
            semantic_hash=semantic_hash,
            plan_hash=plan_hash,
        ),
        integration_spec_preview=spec_preview,
        pipeline_stages=build_pipeline_stages(spec_preview),
        process_cfg=(),
        component_dependencies=build_component_dependencies(spec_preview),
        topology_relations=build_topology_relations(request.topology_spec),
        resolved_references=references,
        validation_report=validation,
        errors=errors,
        warnings=warnings,
        capability_gaps=gaps,
        required_decisions=decisions,
    )
    internals = _PlanInternals(
        normalized=normalized,
        symbols=symbols,
        semantic_hash=semantic_hash,
        plan_hash=plan_hash,
    )
    return result, internals


# ---------------------------------------------------------------------------
# phase 2 — read-only canonical compile
# ---------------------------------------------------------------------------


def build_artifact_descriptors(
    normalized: _NormalizedIntent, symbols: Any
) -> Tuple[Tuple[ArtifactFingerprintV1, ...], Tuple[ProcessCfgSummaryV1, ...]]:
    """Compile every authored process and fingerprint what came out.

    Artifacts are the CANONICAL EMISSION PLAN and the normalized IR — not XML.
    The compiler's production return value is ``(SemanticCfgV1, EmissionPlanV1)``;
    the emission plan is its own deterministic, already-golden-tested canonical
    form, and it is the quantity a compile→apply comparison actually compares.
    (Live drift is a different comparison entirely: apply-time live XML against
    verify-time live XML, both fetched from Boomi — see
    ``compare_live_build_provenance``.)

    Bytes stay internal. Only the digest and the byte length are published.
    """
    from ..compiler.process_ir.contracts import canonical_emission_plan_json
    from ..compiler.process_ir.diagnostics import ProcessIRCompileError
    from ..compiler.process_ir.pipeline import compile_process_ir_v1
    from ..models.process_ir import canonical_process_ir_json, parse_process_ir_v1

    fingerprints: List[ArtifactFingerprintV1] = []
    cfg_summaries: List[ProcessCfgSummaryV1] = []

    for component_key, ir in normalized.process_roots:
        try:
            # Re-parse from the dump for the same reason the validator does: the
            # object may have been built by something other than a strict parse.
            reparsed = parse_process_ir_v1(ir.model_dump(mode="json"))
            cfg, plan = compile_process_ir_v1(reparsed, symbols)
        except ProcessIRCompileError as exc:
            raise AuthoringWorkflowError(
                AUTHORING_COMPILE_BLOCKED,
                tuple(
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        message="Canonical compilation rejected this process.",
                        path=getattr(diagnostic, "path", "") or "",
                        subject_kind="process",
                        subject_id=component_key,
                        # NOT "re-plan". Plan reports this input valid — reference
                        # resolution is not one of the semantic-validation phases
                        # — so telling a caller to re-plan sent them round a loop
                        # they could not exit (issue #146 QA, bug #409). The
                        # failure is usually in the COMPONENT the reported node
                        # references, not in the node as authored.
                        remediation=(
                            "Check the component this step references — a "
                            "connector operation needs connector_type and "
                            "action_type on its component config. Re-planning "
                            "will NOT surface this: reference resolution happens "
                            "at compile, not during semantic validation."
                        ),
                        cause_codes=(getattr(diagnostic, "code", "") or "",),
                    )
                    for diagnostic in (exc.diagnostics or ())
                )
                or (
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        message="Canonical compilation rejected this process.",
                        subject_kind="process",
                        subject_id=component_key,
                    ),
                ),
            ) from None

        plan_text = canonical_emission_plan_json(plan)
        plan_digest, plan_length = artifact_fingerprint(plan_text)
        ir_text = canonical_process_ir_json(reparsed)
        ir_digest, ir_length = artifact_fingerprint(ir_text)

        fingerprints.append(
            ArtifactFingerprintV1(
                component_key=component_key,
                component_type="process",
                artifact_kind="process_ir_emission_plan",
                byte_length=plan_length,
                digest=plan_digest,
            )
        )
        fingerprints.append(
            ArtifactFingerprintV1(
                component_key=component_key,
                component_type="process",
                artifact_kind="process_ir_normalized",
                byte_length=ir_length,
                digest=ir_digest,
            )
        )
        # Terminals are the CFG's own ``exit_node_ids``, not a re-derivation from
        # edge degree: the compiler already decided which nodes exit, and a
        # second opinion computed here could disagree with the artifact it
        # claims to describe.
        exit_ids = set(getattr(cfg, "exit_node_ids", ()) or ())
        terminal_kinds = {
            str(getattr(node.semantic, "semantic_kind", "") or "")
            for node in (getattr(cfg, "nodes", ()) or ())
            if node.node_id in exit_ids
        }
        cfg_summaries.append(
            ProcessCfgSummaryV1(
                component_key=component_key,
                node_count=len(getattr(cfg, "nodes", ()) or ()),
                edge_count=len(getattr(cfg, "edges", ()) or ()),
                terminal_kinds=tuple(sorted(kind for kind in terminal_kinds if kind)),
            )
        )

    return sort_by_key(fingerprints), sort_by_key(cfg_summaries)


def compile_authoring_request_v1(
    request: AuthoringRequestV1,
    *,
    boomi_client: Any = None,
    profile: str,
    account_id: Optional[str] = None,
) -> Tuple[AuthoringCompileResultV1, _PlanInternals]:
    """Canonically compile typed intent. Mutates nothing; returns no build id."""
    plan_result, internals = plan_authoring_request_v1(
        request, boomi_client=boomi_client, profile=profile, account_id=account_id
    )

    # A fatal plan is a compile refusal, not a compile with errors attached. The
    # ordered causative diagnostics travel; no artifact bundle does.
    if plan_result.errors:
        raise AuthoringWorkflowError(AUTHORING_COMPILE_BLOCKED, plan_result.errors)

    unresolved = tuple(d for d in plan_result.required_decisions if not d.resolved)
    if unresolved:
        raise AuthoringWorkflowError(
            AUTHORING_REQUIRED_DECISION_MISSING,
            tuple(
                _diag(
                    AUTHORING_REQUIRED_DECISION_MISSING,
                    "error",
                    message="A required decision is unresolved; compilation is blocked.",
                    path=decision.path,
                    subject_kind="decision",
                    subject_id=decision.decision_id,
                    remediation="Supply decisions[].option_id and recompile.",
                )
                for decision in unresolved
            ),
        )

    fingerprints, cfg_summaries = build_artifact_descriptors(
        internals.normalized, internals.symbols
    )

    normalized_payload = _normalized_payload(internals.normalized, request)
    normalized_digest = sha256_fingerprint(normalized_payload)
    revisions = get_authoring_revisions()
    compile_hash = compile_fingerprint(
        plan_hash=internals.plan_hash,
        normalized_intent_digest=normalized_digest,
        integration_spec=internals.normalized.integration_spec.model_dump(mode="json"),
        artifact_fingerprints=[f.model_dump(mode="json") for f in fingerprints],
        compiler_revision=revisions["compiler_revision"],
        capability_revision=revisions["capability_revision"],
    )

    if (
        request.expected_compile_hash is not None
        and request.expected_compile_hash != compile_hash
    ):
        raise AuthoringWorkflowError(
            AUTHORING_PLAN_STALE,
            (
                _diag(
                    AUTHORING_PLAN_STALE,
                    "error",
                    message=(
                        "The recomputed compile differs from the one this "
                        "request was bound to."
                    ),
                    subject_kind="compile_hash",
                    remediation="Recompile and bind apply to the new compile hash.",
                ),
            ),
        )

    result = AuthoringCompileResultV1(
        revision_binding=_binding(
            profile=profile,
            account_id=account_id,
            semantic_hash=internals.semantic_hash,
            plan_hash=internals.plan_hash,
            compile_hash=compile_hash,
        ),
        integration_spec_preview=plan_result.integration_spec_preview,
        pipeline_stages=plan_result.pipeline_stages,
        process_cfg=cfg_summaries,
        component_dependencies=plan_result.component_dependencies,
        topology_relations=plan_result.topology_relations,
        resolved_references=plan_result.resolved_references,
        validation_report=plan_result.validation_report,
        errors=(),
        warnings=plan_result.warnings,
        capability_gaps=plan_result.capability_gaps,
        required_decisions=plan_result.required_decisions,
        artifact_fingerprints=fingerprints,
        normalized_intent_digest=normalized_digest,
    )
    return result, internals


# ---------------------------------------------------------------------------
# phase 3 — the apply gate
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompiledBundle:
    """A compile the server just reproduced, in this profile, from this payload.

    The typed apply branch takes one of these as a REQUIRED argument. That is
    what makes "apply validates before its first mutation" structural: there is
    no code path from a request to a materializer that does not pass through
    here.
    """

    integration_spec: IntegrationSpecV1
    compile_result: AuthoringCompileResultV1
    request: AuthoringRequestV1


def preflight_typed_apply_v1(
    raw_request_payload: Mapping[str, Any],
    *,
    boomi_client: Any = None,
    profile: str,
    account_id: Optional[str] = None,
) -> CompiledBundle:
    """Reproduce the compile in THIS profile and compare the caller's binding.

    Takes the RAW payload, not a parsed request: re-parsing is the whole point.
    ``ProcessIRV1`` is strict but not frozen, and an object handed down from an
    earlier phase can have been changed since it was checked — which is exactly
    the failure that matters when a mutation is downstream.

    Requires both ``expected_capability_revision`` and ``expected_compile_hash``.
    An apply with no binding is an apply that verified nothing.
    """
    request = AuthoringRequestV1.model_validate(dict(raw_request_payload))

    missing = [
        name
        for name, value in (
            ("expected_capability_revision", request.expected_capability_revision),
            ("expected_compile_hash", request.expected_compile_hash),
        )
        if value is None
    ]
    if missing:
        raise AuthoringWorkflowError(
            AUTHORING_APPLY_VALIDATION_REQUIRED,
            tuple(
                _diag(
                    AUTHORING_APPLY_VALIDATION_REQUIRED,
                    "error",
                    message="A typed apply must bind to a validated compile.",
                    subject_kind="binding",
                    subject_id=name,
                    remediation=(
                        "Run build_integration(action='compile'), then pass its "
                        "capability revision and compile hash to apply."
                    ),
                )
                for name in sorted(missing)
            ),
        )

    # Recompute with ALL THREE of the caller's expectations stripped, so every
    # comparison happens HERE, once, and reports the field that actually
    # differs. Leaving them on made the earlier phases raise first: a
    # cross-profile replay came back as "compile_hash" (compile's own staleness
    # check fired before the scope was ever compared) and a capability mismatch
    # came back as APPLY_VALIDATION_REQUIRED instead of its own code, which
    # ADR-001 §7 gives precedence when the capability revision is what moved.
    recompute_request = request.model_copy(
        update={
            "expected_capability_revision": None,
            "expected_plan_hash": None,
            "expected_compile_hash": None,
        }
    )
    try:
        compile_result, _internals = compile_authoring_request_v1(
            recompute_request,
            boomi_client=boomi_client,
            profile=profile,
            account_id=account_id,
        )
    except AuthoringWorkflowError as exc:
        # A compile that cannot be reproduced is an apply that must not run. The
        # causative diagnostics travel so the caller sees WHY, and the reported
        # code names the phase that refused.
        raise AuthoringWorkflowError(
            AUTHORING_APPLY_VALIDATION_REQUIRED,
            (
                _diag(
                    AUTHORING_APPLY_VALIDATION_REQUIRED,
                    "error",
                    message=(
                        "Apply could not reproduce a valid compile for this "
                        "request; nothing was mutated."
                    ),
                    subject_kind="binding",
                    remediation="Re-plan and recompile, then apply.",
                    cause_codes=(exc.code,),
                ),
            )
            + exc.diagnostics,
        ) from None

    binding = compile_result.revision_binding

    # NOTE ON ACCOUNT SCOPE. There is deliberately no separate
    # ``account_scope_hash`` comparison here. It would compare the active
    # profile's scope hash against itself and could never fail — a check that
    # cannot fire is a false assurance, not a defence.
    #
    # The scope IS bound, transitively and unavoidably: ``account_scope_hash``
    # is a field of the revision binding, the binding is hashed into
    # ``plan_hash``, and ``plan_hash`` is hashed into ``compile_hash``. So a
    # binding minted under profile A cannot satisfy an apply under profile B —
    # it fails below as a ``compile_hash`` mismatch, which is exactly right:
    # under B, that compile genuinely was never produced.

    # Capability mismatch takes PRECEDENCE (ADR-001 §7): when the server's own
    # contract moved, every downstream hash is expected to differ, and reporting
    # a stale plan would send the caller to re-plan against a surface they have
    # not rediscovered yet.
    if binding.capability_revision != request.expected_capability_revision:
        raise AuthoringWorkflowError(
            AUTHORING_CAPABILITY_REVISION_MISMATCH,
            (
                _diag(
                    AUTHORING_CAPABILITY_REVISION_MISMATCH,
                    "error",
                    message=(
                        "This server's authoring capability revision differs from "
                        "the one this apply was bound to; nothing was mutated."
                    ),
                    subject_kind="binding",
                    subject_id="capability_revision",
                    remediation=(
                        "Re-run list_capabilities and get_schema_template, then "
                        "re-plan, recompile, and apply with the new binding."
                    ),
                ),
            ),
        )

    # A plan this server can compile but cannot BUILD must stop here. Letting it
    # through would materialize process XML from the component config while the
    # binding attested to the ProcessIR emission plan — a compile hash certifying
    # an artifact that was never created, which is worse than any refusal.
    blocked = [
        gap
        for gap in compile_result.capability_gaps
        if gap.capability_id == MATERIALIZATION_CAPABILITY
    ]
    if blocked:
        raise AuthoringWorkflowError(
            AUTHORING_APPLY_VALIDATION_REQUIRED,
            tuple(
                _diag(
                    AUTHORING_APPLY_VALIDATION_REQUIRED,
                    "error",
                    message=(
                        "This intent can be planned and compiled but not applied: "
                        "no production path materializes a ProcessIR root."
                    ),
                    path=gap.path,
                    subject_kind="component",
                    subject_id=gap.path.rsplit("/", 1)[-1],
                    remediation=(
                        "Use build_from_archetype or a typed recipe to produce a "
                        "materializable component plan, or apply the legacy "
                        "integration_spec form. Plan and compile remain available "
                        "for this intent."
                    ),
                    cause_codes=(gap.reason_code,),
                )
                for gap in blocked
            ),
        )

    mismatches = []
    if binding.compile_hash != request.expected_compile_hash:
        mismatches.append("compile_hash")
    if request.expected_plan_hash is not None and (
        binding.plan_hash != request.expected_plan_hash
    ):
        mismatches.append("plan_hash")

    if mismatches:
        raise AuthoringWorkflowError(
            AUTHORING_PLAN_STALE,
            tuple(
                _diag(
                    AUTHORING_PLAN_STALE,
                    "error",
                    message=(
                        "The binding this apply carries does not match what this "
                        "server just recomputed; nothing was mutated."
                    ),
                    subject_kind="binding",
                    subject_id=field,
                    remediation=(
                        "Re-plan, recompile, and apply with the new binding. A "
                        "binding minted under a DIFFERENT credential profile "
                        "also fails here: the account scope is part of the hash "
                        "chain, so that compile was never produced in this scope."
                    ),
                )
                for field in sorted(mismatches)
            ),
        )

    return CompiledBundle(
        integration_spec=compile_result.integration_spec_preview,
        compile_result=compile_result,
        request=request,
    )


# ---------------------------------------------------------------------------
# verify-side provenance comparison
# ---------------------------------------------------------------------------


def compare_live_build_provenance(
    provenance: Mapping[str, Any],
    live_component_digests: Mapping[str, str],
) -> LiveDeploymentComparisonV1:
    """Compare live components against what the typed apply recorded.

    The two comparable quantities are apply-time live fingerprints and
    verify-time live fingerprints — BOTH computed from Boomi's own XML. A
    compile-time emission-plan digest is not comparable to live XML at all, and
    comparing them would report drift on every healthy build.

    Revision skew is reported SEPARATELY from component drift: a server upgraded
    since the build is not the same fact as a component someone edited in the UI,
    and one remedy does not fix the other.
    """
    recorded = dict(provenance.get("live_component_fingerprints") or {})
    if not recorded:
        return LiveDeploymentComparisonV1(status="not_requested")

    revisions = get_authoring_revisions()
    binding = dict(provenance.get("revision_binding") or {})
    if binding:
        skew = (
            "match"
            if binding.get("capability_revision") == revisions["capability_revision"]
            else "mismatch"
        )
    else:
        skew = "unknown"

    drifted: List[str] = []
    missing: List[str] = []
    unavailable: List[str] = []
    diagnostics: List[AuthoringDiagnosticV1] = []

    for component_key in sorted(recorded):
        expected_digest = (recorded[component_key] or {}).get("digest")
        observed_digest = live_component_digests.get(component_key)
        if expected_digest is None:
            # No apply-time baseline for this component: the read-back failed
            # when the build was created. There is nothing to compare, and
            # saying "match" for a comparison that never happened is the false
            # clean this whole surface exists to avoid.
            unavailable.append(component_key)
            continue
        if observed_digest is None:
            missing.append(component_key)
            diagnostics.append(
                _diag(
                    AUTHORING_LIVE_DEPLOYMENT_DRIFT,
                    "error",
                    message="A build-owned component could not be read back.",
                    subject_kind="component",
                    subject_id=component_key,
                    remediation="Confirm the component still exists in this account.",
                )
            )
        elif expected_digest and observed_digest != expected_digest:
            drifted.append(component_key)
            diagnostics.append(
                _diag(
                    AUTHORING_LIVE_DEPLOYMENT_DRIFT,
                    "error",
                    message=(
                        "A build-owned component no longer matches the fingerprint "
                        "recorded when this build applied it."
                    ),
                    subject_kind="component",
                    subject_id=component_key,
                    remediation=(
                        "Re-plan and recompile against the current component, or "
                        "revert the out-of-band edit."
                    ),
                )
            )

    if drifted or missing:
        status = "drift"
    elif unavailable:
        # Some component had no baseline, so "everything else matched" is not
        # the same statement as "this build is unchanged".
        status = "unknown"
    else:
        status = "match"
    return LiveDeploymentComparisonV1(
        status=status,
        revision_skew=skew,
        drifted_components=tuple(drifted),
        missing_components=tuple(missing),
        unverifiable_components=tuple(unavailable),
        diagnostics=sort_authoring_diagnostics(tuple(diagnostics)),
    )


__all__ = [
    "AuthoringWorkflowError",
    "CompiledBundle",
    "build_artifact_descriptors",
    "build_component_dependencies",
    "build_integration_spec_preview",
    "build_pipeline_stages",
    "build_resolved_reference_summary",
    "build_topology_relations",
    "compare_live_build_provenance",
    "compile_authoring_request_v1",
    "plan_authoring_request_v1",
    "preflight_typed_apply_v1",
]
