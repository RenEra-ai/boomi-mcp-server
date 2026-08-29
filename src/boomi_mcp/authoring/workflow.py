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

import re as _re
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..errors import (
    AUTHORING_APPLY_VALIDATION_REQUIRED,
    AUTHORING_CAPABILITY_REVISION_MISMATCH,
    AUTHORING_COMPILE_BLOCKED,
    AUTHORING_LIVE_DEPLOYMENT_DRIFT,
    AUTHORING_PLAN_STALE,
    AUTHORING_REQUIRED_DECISION_MISSING,
    PROCESS_COMPONENT_SCHEMA_INVALID,
    PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY,
    PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE,
)
from .process_materialization import (
    canonical_plan_material,
    envelope_relocatability_offenders,
)
from ..models.authoring_workflow import (
    ArtifactFingerprintV1,
    AuthoringCompileResultV1,
    AuthoringDiagnosticV1,
    AuthoringPlanResultV1,
    AuthoringRequestV1,
    parse_authoring_request_v1,
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
    node_identity: str = "",
    evidence: Tuple[Any, ...] = (),
    authoring_contract_entry_ids: Tuple[str, ...] = (),
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
        node_identity=node_identity,
        evidence=tuple(evidence),
        authoring_contract_entry_ids=tuple(sorted(set(authoring_contract_entry_ids))),
    )


def _safe_evidence(finding: Any) -> Tuple[Any, ...]:
    """Re-validate the validator's evidence at the public boundary (#146).

    RE-validated, not forwarded. The compiler's evidence model already enforces
    a closed key allowlist and a token/code value shape, but this layer owns
    what it publishes: trusting the upstream model would mean a widened
    allowlist there silently widens what reaches an MCP response here.

    A pair that fails re-validation is DROPPED rather than raising. Evidence is
    supplementary — losing one pair costs a caller some context, while failing
    the whole plan over it would turn a formatting problem into an outage.
    """
    from ..models.authoring_workflow import AuthoringEvidenceV1

    out = []
    for item in getattr(finding, "evidence", ()) or ():
        try:
            out.append(AuthoringEvidenceV1(key=item.key, value=item.value))
        except Exception:  # noqa: BLE001 — supplementary context, never fatal
            continue
    return tuple(sorted(out, key=lambda pair: pair.sort_key))


#: Appended to every compile-phase remediation. Kept SEPARATE from the compiler's
#: own per-code text rather than replacing it: the per-code text says what to
#: fix, and this says where the failure was NOT found. Both are needed — bug #409
#: was a caller told to "re-plan" a payload that plan reports as valid.
_COMPILE_PHASE_NOTE = (
    " Re-planning will NOT surface this: reference resolution happens at "
    "compile, not during semantic validation, so the fix is usually in the "
    "COMPONENT the reported node references rather than in the node itself."
)


def _compile_remediation(diagnostic: Any) -> str:
    own = (getattr(diagnostic, "remediation", "") or "").strip()
    if not own:
        return _COMPILE_PHASE_NOTE.strip()
    return own + _COMPILE_PHASE_NOTE


#: The headline when the compiler names no message of its own.
_COMPILE_GENERIC_MESSAGE = "Canonical compilation rejected this process."


def _compile_message(diagnostic: Any) -> str:
    """The compiler's OWN message for this diagnostic, or the generic headline.

    The compile phase used to hardcode the generic string for every diagnostic
    while forwarding only the remediation, so the authority's actual statement —
    "no symbol resolves this authored reference", "emission plan is invalid" —
    never reached the caller, and `_authoring_error_envelope` put the generic
    string in the envelope's top-level `error` as the HEADLINE they read first.
    The semantic phase forwards `finding.message` verbatim; this makes compile
    match it.

    Safe for the same reason the semantic forwarding is: every `message=` under
    `compiler/process_ir/` is a static literal or a `_MESSAGES` table lookup —
    no f-string, no interpolation — so a compiler message cannot carry an
    authored value.
    """
    own = (getattr(diagnostic, "message", "") or "").strip()
    return own or _COMPILE_GENERIC_MESSAGE


def _contract_ids_for(code: str) -> Tuple[str, ...]:
    """The served authoring entries that explain ``code``.

    Best-effort: a diagnostic must still reach the caller if the projection
    cannot be built, because the diagnostic is the thing that blocks their
    compile and the citation is the thing that helps them fix it.
    """
    try:
        from .process_ir_projection import authoring_contract_entry_ids_for_diagnostic

        return authoring_contract_entry_ids_for_diagnostic(code)
    except Exception:  # noqa: BLE001 — advisory citation, never fatal
        return ()


# ---------------------------------------------------------------------------
# normalization: intent -> (component plan, process roots, topology, gaps)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _NormalizedIntent:
    #: Since #153 the process roots live IN the spec, as
    #: ``integration_spec.processes`` — one representation, not two. Before that
    #: they travelled alongside it as a parallel tuple, which meant a root could
    #: be present in one and absent from the other with nothing to notice.
    integration_spec: IntegrationSpecV1
    gaps: Tuple[CapabilityGapV1, ...]
    connector_metadata: Mapping[str, Tuple[Optional[str], Optional[str]]]

    @property
    def process_roots(self) -> Tuple[Tuple[str, Any], ...]:
        """``(component_key, ProcessIRV1)`` sorted by key.

        DERIVED from the spec rather than stored beside it, so the two cannot
        disagree. Sorted rather than authored-order because the semantic hash is
        computed over this projection, and a hash that depends on the order a
        caller happened to list their roots is not a fingerprint — the same two
        roots swapped would bind to a different revision and re-plan for nothing.
        The apply ORDER is unaffected: that comes from ``_topological_order``,
        never from list position.
        """
        return tuple(
            (unit.envelope.component_key, unit.process_ir)
            for unit in sorted(
                self.integration_spec.processes,
                key=lambda unit: unit.envelope.component_key,
            )
        )


def _action_type_from_config(config: Mapping[str, Any]) -> Optional[str]:
    """The connector ACTION a component plan declares, family-conditionally.

    Reading `config["action_type"]` alone was wrong: no primitive writes that
    key into a component config. `db_extract`/`db_write` write
    `operation_mode`, the REST primitives add `method`, the SOAP primitives
    write `operation_mode: "execute"`, and `action_type` appears only inside
    `emit_fragment` — a different structure. So every ProcessIR document over a
    real component plan came back with no action at all: a `connector_call` was
    rejected as an unsupported action, and `source`/`target` failed with
    "operation symbol is missing derived connector metadata".

    Families are resolved through the CANONICAL resolvers, the same ones
    `integration_builder._classify_connector_action` uses (an earlier version
    of this docstring cited `_connector_ref_expectations`, which does not exist) — not by matching
    substrings of the connector type, which is how a first version silently
    excluded SOAP: `soap_client` contains neither "rest" nor "database", so it
    fell through to a `None` the compiler then reported as an unsupported
    action, citing a contract entry that publishes it as supported. The REST and
    SOAP families are matched by resolver rather than by name; `database` is
    still an inline literal here, exactly as it is in
    `_classify_connector_action`.

    Two families are deliberately NOT derived. `wss` (the listener family) and
    plain `http` fall through to `None`, and that is the correct answer: they
    are absent from the connector-call allowlist, so the compiler refuses them
    with `..._CONNECTOR_ACTION_UNSUPPORTED` and cites the entries that ARE
    supported. That is an honest refusal, and the opposite of the SOAP bug —
    there the contract published the action as supported while the derivation
    silently produced nothing. So this consults two resolvers, not the three
    `_classify_connector_action` uses; the set is deliberate, not exhaustive,
    and an earlier version of this docstring claimed otherwise.

    `action_type` stays an accepted alias because the legacy path accepts it —
    except that the SOAP builder REJECTS it as an unsupported operation field,
    so for SOAP the derivation is the only way through.
    """
    from ..categories.components.builders.connector_builder import (
        _resolve_rest_connector_type,
        _resolve_soap_client_connector_type,
    )

    declared = config.get("action_type") or config.get("actionType")
    declared = declared.strip() if isinstance(declared, str) else ""

    connector_type = config.get("connector_type")
    family = connector_type.strip().lower() if isinstance(connector_type, str) else ""

    mode = config.get("operation_mode")
    mode = mode.strip().lower() if isinstance(mode, str) else ""

    if family == "database":
        # The emitter writes these mixed-case verbs; the DB source's `Get`
        # mirrors the write target's `Send`.
        if mode == "get":
            return "Get"
        if mode == "send":
            return "Send"
        return declared or None

    if _resolve_rest_connector_type(connector_type) is not None:
        method = config.get("method")
        if isinstance(method, str) and method.strip():
            return method.strip().upper()
        return declared.upper() if declared else None

    if _resolve_soap_client_connector_type(connector_type) is not None:
        # SOAP Client exposes a single EXECUTE action and no per-request verb.
        if mode == "execute":
            return "EXECUTE"
        return declared.upper() if declared else None

    return declared or None


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
        action_type = _action_type_from_config(config)
        if connector_type or action_type:
            metadata[component.key] = (
                str(connector_type) if connector_type else None,
                str(action_type) if action_type else None,
            )
    return metadata


def _reparsed_unit(unit):
    """A unit whose nested ProcessIR is SERVER-OWNED, not the caller's object.

    §6 AR2-01. `ProcessAuthoringUnitV1` is frozen, but the `ProcessIRV1` it
    holds is not: an in-process caller can hand over a parsed unit and mutate
    its body afterwards. Everything downstream then dumps that mutated model —
    the semantic validator's snapshot, the compile reparse, the semantic-hash
    payload — and pydantic renders the caller's authored content, secrets
    included, into a serializer warning on the way. Reparsing here means those
    dumps operate on an object this server validated.

    `warnings=False` is load-bearing for the same reason the composer arm gives:
    the dump that FEEDS the reparse is itself a dump of the mutated model.
    """
    from ..models.process_ir import (
        ProcessIRValidationError, parse_process_ir_v1,
    )

    try:
        reparsed = parse_process_ir_v1(
            unit.process_ir.model_dump(mode="json", warnings=False)
        )
    except ProcessIRValidationError as exc:
        # Translated into this module's own refusal channel rather than let out
        # raw: a caller of `plan`/`compile` branches on `AuthoringWorkflowError`
        # and its diagnostics, and an unmodelled escape from an intake helper is
        # a second error contract for the same surface. The message is the
        # parser's value-free text; the mutated content never travels.
        raise AuthoringWorkflowError(
            AUTHORING_COMPILE_BLOCKED,
            (
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "error",
                    message=(
                        "A process root failed canonical re-parse at intake."
                    ),
                    subject_kind="process",
                    subject_id=unit.envelope.component_key,
                    remediation=(
                        "Author the root as a valid ProcessIR document; the "
                        "server re-parses every caller-supplied root before "
                        "compiling it."
                    ),
                    cause_codes=(PROCESS_COMPONENT_SCHEMA_INVALID,),
                ),
            ),
        ) from exc
    return unit.model_copy(update={"process_ir": reparsed})


def _normalize_intent(request: AuthoringRequestV1) -> _NormalizedIntent:
    intent = request.intent
    kind = intent.intent_kind

    if kind == "integration_spec":
        spec = intent.integration_spec
        # THIS ARM REPARSES ITS UNITS TOO (§6 AR3-01). The AR2-01 sweep covered
        # the mechanism "a warning-enabled dump of a caller-reachable model" and
        # fixed the direct arm's intake; it never asked the other question — WHICH
        # ARMS hand a caller-owned nested IR to the compiler. This one does: the
        # spec is returned verbatim, so a caller who mutates a unit's IR after
        # building the request gets a raw pydantic ValidationError out of the
        # semantic validator's snapshot, carrying the mutated value. Reparsing
        # here makes the object server-owned, exactly as at the direct arm.
        #
        # REPARSE ONLY, never sort: the caller authored this spec and the
        # semantic-hash payload dumps it wholesale, so reordering would move the
        # hash for any multi-root spec authored out of key order.
        if spec.processes:
            spec = spec.model_copy(update={
                "processes": tuple(_reparsed_unit(unit) for unit in spec.processes)
            })
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
            gaps=gaps,
            connector_metadata=_connector_metadata_from_components(spec.components),
        )

    if kind == "process_ir":
        # (helper defined at module scope; see `_reparsed_unit`)
        # The units go into the spec SORTED by key. `_NormalizedIntent.process_roots`
        # sorts its own projection too, but the spec is what the semantic-hash
        # payload dumps wholesale, so canonicalizing once here is what actually
        # makes the fingerprint independent of the order the caller listed them.
        # ...and each unit's nested IR is REPARSED before it enters the spec
        # (§6 AR2-01). A frozen outer model does not make the nested
        # `ProcessIRV1` immutable, so an in-process caller can hand over a
        # parsed unit and then mutate its body — and every downstream dump then
        # renders the caller's authored content, secrets included, into a
        # pydantic serializer warning. The composer arm already reparses for
        # exactly this reason; this is the same fix on the direct arm, so the
        # objects the compiler and validator dump are server-owned.
        spec = IntegrationSpecV1(
            name=intent.integration_name,
            components=list(intent.components),
            processes=sorted(
                (_reparsed_unit(unit) for unit in intent.units),
                key=lambda unit: unit.envelope.component_key,
            ),
        )
        return _NormalizedIntent(
            integration_spec=spec,
            gaps=(),
            connector_metadata=_connector_metadata_from_components(intent.components),
        )

    # kind == "recipe"
    return _normalize_recipe_intent(
        intent, request.effect_declarations, intent.conflict_policy
    )


def _normalize_recipe_intent(
    intent, effect_declarations=None, conflict_policy: str = "reuse"
) -> _NormalizedIntent:
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
        # #154 (QA-154-r1-01, second half). A recipe-kind intent assembles AND
        # compiles here, inside normalization — before `_validate_processes` has
        # seen anything. So the declarations must reach the recipe engine at this
        # point or that intent kind can never use the channel at all, no matter
        # what the later sites do.
        result = run_recipes(
            requests,
            catalog=MaterializationCatalog(catalog_entries),
            connector_metadata=connector_metadata,
            effect_declarations=effect_declarations,
            conflict_policy=conflict_policy,
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
    roots = tuple(sorted(result.composed.process_roots, key=lambda pair: pair[0]))
    supporting, units = _lift_recipe_roots_into_units(components, roots)
    spec = IntegrationSpecV1(
        name=intent.integration_name,
        components=supporting,
        processes=units,
    )
    return _NormalizedIntent(
        integration_spec=spec,
        gaps=(),
        # Connector metadata is derived from the components the recipe emitted,
        # INCLUDING the process entries that were just lifted into units: the
        # projection keys on component type, and a lifted process contributes no
        # connector family, so passing the full list keeps the mapping identical
        # to what it was before the lift.
        connector_metadata=_connector_metadata_from_components(components),
    )


def _json_pointer(dotted: str) -> str:
    """``a.b[0].c[1]`` -> ``/a/b/0/c/1``.

    The builder layer reports a location in bracket notation; a served
    diagnostic `path` is a JSON pointer. Converting only the dots left
    `/process_extensions/connections[0]/fields[0]`, so a consumer following the
    path looked for a key literally named `connections[0]` (Codex round 3).
    """
    return "/" + "/".join(
        part for part in _re.split(r"[.\[\]]+", str(dotted)) if part
    )


def _extension_bindings_from_config(raw):
    """Legacy recipe extension config -> the typed ``ProcessExtensionBindingsV1``.

    The NORMALIZATION BOUNDARY between the legacy reader's tolerance and the
    typed models' strictness — and it DELEGATES the tolerance question rather
    than answering it.

    ``extension_bindings_from_legacy_config`` is the authority on which shapes
    are acceptable, and it already normalizes them: it strips a padded
    ``connection_id``/``id``/``xpath``, leaves ``label`` byte-exact because the
    renderer emits it verbatim, and refuses twelve distinct malformed shapes with
    ``PROCESS_EXTENSIONS_INVALID``.

    An earlier version of this function hand-wrote a SUBSET of those refusals,
    and Codex round 2 found both halves of what that costs: it rejected
    ``process_extensions=[]``, which the legacy reader accepts as an explicit
    no-op, and it accepted ``{"connection": [...]}`` / ``{"connections": null}``
    / ``{"connections": {}}``, which the legacy reader refuses — so a misspelled
    key still silently dropped every requested environment override and deployed
    a successful-looking process without them. A hand-copy of an authority is
    wrong in both directions at once; asking the authority is not.
    """
    from ..categories.components.process_component_materializer import (
        extension_bindings_from_legacy_config,
    )
    from ..models.process_component import (
        ProcessConnectionOverrideV1,
        ProcessExtensionBindingsV1,
        ProcessOverrideFieldV1,
    )

    try:
        normalized = extension_bindings_from_legacy_config({"process_extensions": raw})
    except Exception as exc:  # noqa: BLE001 — re-raised as an authoring diagnostic
        raise AuthoringWorkflowError(
            AUTHORING_COMPILE_BLOCKED,
            (
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "error",
                    message=(
                        "The process extension bindings are malformed and would "
                        "otherwise be dropped silently."
                    ),
                    path=_json_pointer(
                        getattr(exc, "field", "") or "process_extensions"
                    ),
                    subject_kind="process",
                    remediation=(
                        'Shape: {"connections": [{"connection_id": "...", '
                        '"fields": [{"id": "...", "label": "..."}]}]}.'
                    ),
                    cause_codes=(str(getattr(exc, "error_code", "") or ""),),
                ),
            ),
        ) from None

    connections = []
    for entry in normalized:
        fields = tuple(
            ProcessOverrideFieldV1(
                **{k: v for k, v in field.items() if k in ("id", "label", "xpath")}
            )
            for field in entry.get("fields") or ()
        )
        kwargs = {"connection_id": entry["connection_id"], "fields": fields}
        if entry.get("connector_type"):
            kwargs["connector_type"] = entry["connector_type"]
        connections.append(ProcessConnectionOverrideV1(**kwargs))
    return ProcessExtensionBindingsV1(connections=tuple(connections))


def _lift_recipe_roots_into_units(components, roots):
    """Pair each composed ProcessIR root with the process component describing it.

    An interim bridge, and deliberately labelled one: #159 migrates recipe /
    composition authoring to author units directly. Until then a recipe still
    emits its process as an ``IntegrationComponentSpec`` alongside a composed
    root, and this is where those two halves become the single
    :class:`ProcessAuthoringUnitV1` the canonical chain requires.

    Returns ``(supporting_components, units)``. The lifted process entries are
    REMOVED from the component list: leaving them in both places would put one
    process in two tuples of one shared key namespace, which the spec validator
    correctly rejects — and, worse, would make it ambiguous which of the two
    descriptions apply should build from.

    ``process_kind`` is never read. That is the whole point of the milestone: a
    recipe's process is materialized from its ProcessIR root through the
    canonical chain, not by resolving a legacy dialect.
    """
    from ..models.process_component import (
        ProcessAuthoringUnitV1,
        ProcessComponentEnvelopeV1,
    )

    #: Config keys promoted onto the typed envelope. An ALLOWLIST, not a
    #: passthrough: every other config key belongs to the legacy component
    #: surface and must not silently become envelope contract.
    _ENVELOPE_CONFIG_KEYS = ("description", "folder_name", "process_extensions")

    by_key = {}
    for component in components:
        if component.type == "process":
            by_key.setdefault(component.key, []).append(component)

    units = []
    lifted_keys = set()
    for component_key, ir in roots:
        matches = by_key.get(component_key, ())
        # Reference-only entries describe an EXISTING component to reuse; they
        # author no XML, so they are not a root's envelope and must stay in
        # `components`.
        authored = [
            component
            for component in matches
            if not (component.config or {}).get("reference_only")
        ]
        if len(authored) != 1:
            raise AuthoringWorkflowError(
                AUTHORING_COMPILE_BLOCKED,
                (
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        message=(
                            "A composed process root must correspond to exactly "
                            "one authored process component with the same key."
                        ),
                        subject_kind="process",
                        subject_id=component_key,
                        remediation=(
                            "The recipe emitted "
                            f"{len(authored)} authored process components for this "
                            "root. Emit exactly one."
                        ),
                        cause_codes=(
                            PROCESS_COMPONENT_SCHEMA_INVALID_CARDINALITY,
                        ),
                    ),
                ),
            )
        component = authored[0]
        config = component.config or {}

        # `name` AND `action` must be CALLER-AUTHORED, checked via
        # `model_fields_set` (§6 review AR1-08; plan §8 verbatim: "inspect
        # `model_fields_set` rather than accepting the default silently").
        # The previous version fell back to `config.get("component_name")` —
        # a key the plan's allowlist does not include — and honoured the
        # model's `action="create"` default, so a recipe that authored
        # neither still lifted into a unit whose two REQUIRED envelope fields
        # nobody wrote.
        authored_fields = component.model_fields_set
        if "action" not in authored_fields:
            raise AuthoringWorkflowError(
                AUTHORING_COMPILE_BLOCKED,
                (
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        message=(
                            "A composed process root must author its own "
                            "'action'; the model default is not accepted."
                        ),
                        subject_kind="process",
                        subject_id=component_key,
                        remediation=(
                            "Set action='create' or action='update' explicitly "
                            "on the process component this root belongs to."
                        ),
                        cause_codes=(PROCESS_COMPONENT_SCHEMA_INVALID,),
                    ),
                ),
            )
        name = component.name if "name" in authored_fields else None
        if not name or not str(name).strip():
            raise AuthoringWorkflowError(
                AUTHORING_COMPILE_BLOCKED,
                (
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        message=(
                            "A composed process root needs a component name to "
                            "materialize; the recipe supplied none."
                        ),
                        subject_kind="process",
                        subject_id=component_key,
                        remediation=(
                            "Emit a non-blank 'name' on the process component "
                            "this root belongs to."
                        ),
                        cause_codes=(PROCESS_COMPONENT_SCHEMA_INVALID,),
                    ),
                ),
            )

        envelope_kwargs = {
            "component_key": component_key,
            "name": str(name),
            # Caller-authored, verified above via `model_fields_set`.
            "action": component.action,
            "depends_on": tuple(component.depends_on or ()),
        }
        if component.component_id:
            envelope_kwargs["component_id"] = component.component_id
        for key in _ENVELOPE_CONFIG_KEYS:
            value = config.get(key)
            if value in (None, "", {}, []):
                continue
            if key == "process_extensions":
                envelope_kwargs[key] = _extension_bindings_from_config(value)
            else:
                envelope_kwargs[key] = value

        units.append(
            ProcessAuthoringUnitV1(
                envelope=ProcessComponentEnvelopeV1(**envelope_kwargs),
                process_ir=ir,
            )
        )
        lifted_keys.add(component_key)

    supporting = [
        component
        for component in components
        if not (
            component.type == "process"
            and component.key in lifted_keys
            and not (component.config or {}).get("reference_only")
        )
    ]
    return supporting, units


# ---------------------------------------------------------------------------
# read-only evidence gathering
# ---------------------------------------------------------------------------


def build_integration_spec_preview(normalized: _NormalizedIntent) -> IntegrationSpecV1:
    """The ComponentPlan preview — explicitly an ``IntegrationSpecV1``.

    Named a preview and not a plan: it is what apply WOULD materialize, and
    calling it the plan invites reading it as something already true.

    **The authored ProcessIR roots are WITHHELD from this projection (#153).**

    This return value is SERVED — echoed as
    ``authoring_result.integration_spec_preview`` and as the legacy
    ``integration_spec`` envelope — and ADR-001 §11 is explicit that results
    carry hashes, opaque references and value-free diagnostics, never authored
    payload. Before #153 that held for free: the roots travelled BESIDE the spec,
    so the spec echo contained only components. Moving them INTO the spec made
    the echo replay every authored step verbatim. Measured with the clean-room
    watermark, the authored value appeared in exactly two served fields and in
    zero diagnostics — so the diagnostics stayed value-free and the SPEC ECHO was
    the whole of the regression. A ``set_property`` value or a scripting step is
    caller content that can carry a credential, and replaying it through a
    logged, cached, LLM-visible response is a real weakening of the secrets
    posture even though the caller is the one who sent it.

    **Why the roots are dropped rather than redacted.** There is no such thing as
    an empty ``ProcessIRV1``: the body's cardinality rule refuses a lone ``stop``,
    a lone ``return_documents``, and a ``message`` + terminal pair alike (measured
    — all four candidate placeholders raised
    ``PROCESS_IR_SCHEMA_INVALID_CARDINALITY`` at ``/body``). Any stand-in root
    would therefore have to be a fabricated source/target the caller never wrote,
    served inside a field named "preview" — a lie that reads as fact. Dropping
    the tuple says exactly what is true: this projection does not describe the
    semantics.

    **Nothing is lost.** The roots remain fully present in the INTERNAL
    normalized spec that compilation, fingerprinting and apply all read; only
    this served projection withholds them. And the served result still describes
    every root, value-free and by key, through ``process_cfg`` summaries
    (``ProcessCfgSummaryV1.component_key`` plus node/edge counts) and the
    per-root artifact fingerprints — both computed from the REAL root, which is
    what makes them evidence rather than an echo.
    """
    return _withhold_process_roots(normalized.integration_spec)


def _withhold_process_roots(spec: IntegrationSpecV1) -> IntegrationSpecV1:
    """Drop the authored ProcessIR roots from a SERVED spec projection.

    One function, called at every point a spec becomes a served preview, because
    #153's first attempt withheld the roots in the preview BUILDER and was then
    silently undone fifty lines later by the legacy component-plan echo — which
    is rebuilt from the normalized spec and therefore carries the roots back in.
    QA measured planted canaries in every served plan and compile response while
    the builder itself was provably withholding them.

    Idempotent, so the belt-and-braces second call is safe.
    """
    if not spec.processes:
        return spec
    return spec.model_copy(update={"processes": ()})


def build_component_dependencies(
    spec: IntegrationSpecV1,
) -> Tuple[ComponentDependencyEdgeV1, ...]:
    """ComponentPlan materialization edges — not CFG edges, not topology relations.

    Covers BOTH participant families (Codex round 1). It walked `spec.components`
    alone, so a process root declaring four supporting components contributed
    zero edges and the served summary described an empty dependency graph for a
    plan whose execution order enforces all four. The edge summary is value-free
    — keys only — so it is unaffected by the root withholding that protects the
    authored ProcessIR bodies, and it must be built from the internal spec
    BEFORE that withholding rather than from the projection afterwards.
    """
    edges = [
        ComponentDependencyEdgeV1(component_key=component.key, depends_on=dependency)
        for component in spec.components
        for dependency in (component.depends_on or ())
    ]
    edges += [
        ComponentDependencyEdgeV1(
            component_key=unit.envelope.component_key, depends_on=dependency
        )
        for unit in (getattr(spec, "processes", ()) or ())
        for dependency in (unit.envelope.depends_on or ())
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


@dataclass(frozen=True)
class _ParticipantView:
    """A canonical process root, seen as a DECLARED reference target (#153).

    Deliberately not an ``IntegrationComponentSpec``. Three reasons, in order of
    weight:

    1. The #149 reachability census enumerates every site that produces a
       process-typed component spec, so #160 can retire the legacy process
       surface with a complete list. Minting one here — purely as an internal
       shim — would have added a row to that inventory for a construct that is
       never materialized and never carries ``process_kind``. Measured: the
       census flagged exactly that when this projection first used the real
       model.
    2. A canonical root IS NOT a legacy component. Reusing the legacy model to
       describe one blurs the distinction this milestone exists to draw.
    3. It exposes only what the lookup below reads, so it cannot accidentally
       acquire component semantics later.

    ``type`` is fixed and ``config`` is empty so the existing read-only
    metadata lookup (``_metadata_type_for_component`` /
    ``_resolve_existing_components``) works unchanged by duck typing.
    """

    key: str
    name: Optional[str]
    component_id: Optional[str]
    type: str = "process"
    config: Mapping[str, Any] = field(default_factory=dict)


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
    # #153: canonical process roots are declared participants too. They live in
    # `spec.processes`, not among the components, so without this projection a
    # `$ref` naming a root — a `process_call`, or an API Service route target —
    # is absent from the summary entirely and reads as dangling in a plan that
    # is complete.
    #
    # Projected as `_ParticipantView`, NOT as an `IntegrationComponentSpec`.
    # Building a real component spec here was the first attempt and the #149
    # reachability census immediately flagged it as a new `process_kind_producer`
    # — correctly: that census enumerates every site that produces a
    # process-typed component spec so #160 can retire the legacy surface, and a
    # synthetic internal shim would have grown exactly the inventory #160 must
    # shrink. The view is also the more honest object: a canonical root is not a
    # legacy component, and only the four read-only attributes the lookup below
    # consults are exposed.
    for unit in spec.processes:
        envelope = unit.envelope
        declared.setdefault(
            envelope.component_key,
            _ParticipantView(
                key=envelope.component_key,
                name=envelope.name,
                component_id=envelope.component_id,
            ),
        )

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


#: #154. Static, code-selected text for an effect-declaration rejection. No
#: authored value is ever interpolated: a declaration names components, digests
#: and property names, and none of those belong in a served diagnostic.
_EFFECT_DECLARATION_MESSAGES = {
    "unbound": (
        "this effect declaration does not bind to anything in the request"
    ),
    "unresolved-or-wrong-type": (
        "this effect declaration names a component that does not resolve, or "
        "resolves to the wrong component type"
    ),
    "unbound-or-digest-mismatch": (
        "this script effect declaration does not match any script in the "
        "request: either no step carries it, or the recomputed digest differs"
    ),
    "content-mismatch": (
        "this effect declaration disagrees with what the server derived from "
        "the resolved component"
    ),
    "duplicate-binding": (
        "two effect declarations in this family name the same component; "
        "identity is the component, not the reference spelling"
    ),
}

_EFFECT_DECLARATION_REMEDIATIONS = {
    "unbound": (
        "Remove the declaration, or author the step it describes. An external "
        "writer contract binds to any cache_get naming that cache; authoring "
        "external_writer on it governs whether the missing-writer error may "
        "downgrade, not whether the declaration binds."
    ),
    "unresolved-or-wrong-type": (
        "Declare the component in the component plan and reference it by "
        "logical key with '$ref:KEY'."
    ),
    "unbound-or-digest-mismatch": (
        "Recompute source_sha256 from the exact script source in the request, "
        "with no whitespace or newline normalization."
    ),
    "content-mismatch": (
        "Effect content comes from server-side inspection and the vetted script "
        "registry, never from the declaration. Match the derived effect, or omit "
        "the declaration and accept the strict findings."
    ),
    "duplicate-binding": (
        "Keep ONE declaration per component. Two references may legally name the "
        "same component, so declaring both is a duplicate even though the "
        "reference strings differ."
    ),
}


def _literal_profile_indexes(boomi_client: Any, normalized: Any):
    """Field indexes for literal existing-profile UUIDs, or None.

    #179. Reuses the integration builder's own resolver — the same one the plan
    path uses — rather than modelling profile-index resolution a second time.

    NOT gated on a live client. That resolver checks the caller's own
    `profile_indexes_by_component_id` BEFORE attempting discovery, and its
    discovery helper already returns None on any failure, so a null client
    still resolves every index the caller supplied. Gating on the client threw
    those away and sent offline planning straight back to the deferral — which
    could then accept an effect for a map the caller's OWN index disproves,
    and offline plans skip the legacy echo that would otherwise catch it.
    """
    try:
        from ..categories.integration_builder import _resolve_literal_profile_indexes

        return _resolve_literal_profile_indexes(
            boomi_client, normalized.integration_spec
        ) or None
    except Exception:  # noqa: BLE001 - discovery is best effort; absence defers
        return None


def _validate_processes(
    normalized: _NormalizedIntent,
    declarations: Any = None,
    conflict_policy: str = "reuse",
    literal_indexes: Any = None,
) -> Tuple[ValidationReportSummaryV1, Tuple[AuthoringDiagnosticV1, ...], Any, Any]:
    """Run the unified #143 semantic validator over every authored process.

    Uses ``validate_process_ir``, which REPORTS and does not raise on a bad
    payload — planning must be able to hand a caller everything wrong with their
    intent at once, not the first thing that stopped it.
    """
    from ..compiler.process_ir.semantic_validation.pipeline import validate_process_ir
    from ..recipes.materialization import build_symbol_table

    # The snapshot rides along on the TYPED route too. Wiring only the raw route
    # left every `requires_path_binding` unset here, so the blank-path refusal
    # fired at wet apply and on no pre-apply surface at all — plan, compile and
    # `dry_run` each returned success for a document the wet apply then refused.
    # Live QA measured that; the unit tests could not see it, because they build
    # the symbol table directly rather than through the route a caller uses.
    #
    # This half needs NO account: it resolves from the request's own components,
    # which is exactly what a pre-apply surface is allowed to know. The live
    # reading stays at apply, where there is an account to read.
    from .connector_resolution_snapshot import (
        ConnectorIdentityError,
        build_connector_resolution_snapshot,
    )

    # THIS SURFACE REPORTS; it does not refuse. That is not a preference, it is
    # this function's stated contract one docstring above: planning hands a
    # caller everything wrong with their intent at once, never the first thing
    # that stopped it. So the identical condition gets two dispositions decided
    # by the SURFACE — a diagnostic here, a refusal at apply, where the next
    # thing that happens is a write. Letting the raise through turned `plan`
    # into a refuser and emptied the report a caller was told to read.
    snapshot_diagnostics: List[AuthoringDiagnosticV1] = []
    try:
        snapshot = build_connector_resolution_snapshot(
            normalized.integration_spec.components
        )
    except ConnectorIdentityError as snapshot_error:
        # EVERY failure, and the identities that DID resolve. Reporting only the
        # first contradicts this surface's report-all contract as squarely as
        # raising did; discarding the siblings suppressed diagnostics for
        # components that were never in question — including the blank-path
        # refusal, which is snapshot-derived.
        snapshot = snapshot_error.partial or build_connector_resolution_snapshot(())
        for failure in snapshot_error.failures:
            snapshot_diagnostics.append(
                _diag(
                    failure.code,
                    "error",
                    message=str(failure),
                    subject_kind="component",
                    subject_id=failure.component_key,
                    remediation=(
                        "Supply component XML this server can parse and that names "
                        "one operation type, or omit it and declare the "
                        "component's configuration instead."
                    ),
                )
            )
    symbols = build_symbol_table(
        list(normalized.integration_spec.components),
        # #153: the roots are participants too. Without them a `$ref` naming
        # another root resolves to nothing and a complete plan reports a
        # dangling reference.
        process_keys=[
            unit.envelope.component_key
            for unit in normalized.integration_spec.processes
        ],
        connector_metadata=normalized.connector_metadata,
        connector_resolution_snapshot=snapshot,
    )

    # SEEDED, not merely appended. A diagnostic that contributes no code and no
    # error leaves the served summary saying `is_valid: false` with an empty code
    # list, so a caller classifying by the summary cannot tell WHY — which is the
    # one thing a summary exists to say.
    errors = len(snapshot_diagnostics)
    warnings = 0
    advisories = 0
    codes: List[str] = [d.code for d in snapshot_diagnostics]
    diagnostics: List[AuthoringDiagnosticV1] = list(snapshot_diagnostics)

    # #154. Effect declarations are resolved ONCE, here, before any root is
    # validated: identity is checked against the symbol table, effect CONTENT is
    # derived server-side, and each root gets only the contracts that bind inside
    # it. When the caller declared nothing this returns `None` per root, which is
    # the pre-#154 argument exactly.
    from .process_ir_effects import resolve_process_ir_effect_declarations

    resolution = resolve_process_ir_effect_declarations(
        normalized.process_roots,
        declarations,
        symbols,
        list(normalized.integration_spec.components),
        child_roots={
            "$ref:" + key: root for key, root in normalized.process_roots
        },
        conflict_policy=conflict_policy,
        # #179. The plan's own profile indexes. Without them the effect gate
        # asked the plan authority a question it answered "index unavailable",
        # and that early refusal MASKED every check ordered after it — plus the
        # index-CONDITIONAL ones ordered before it, which the authority skips
        # when the index is None. A map the plan refuses could therefore derive
        # a trusted effect.
        literal_indexes=literal_indexes,
    )
    for finding in resolution.findings:
        errors += 1
        codes.append(finding.code)
        diagnostics.append(
            _diag(
                finding.code,
                "error",
                message=_EFFECT_DECLARATION_MESSAGES[finding.reason],
                path=finding.path,
                subject_kind="process",
                subject_id="",
                remediation=_EFFECT_DECLARATION_REMEDIATIONS[finding.reason],
            )
        )

    for component_key, ir in normalized.process_roots:
        # Pass the keyword ONLY when this root actually has trusted context.
        # `capabilities=None` would override the strict default rather than fall
        # back to it, which is the opposite of fail-closed.
        root_capabilities = resolution.capabilities_by_root.get(component_key)
        report = (
            validate_process_ir(ir, symbols, capabilities=root_capabilities)
            if root_capabilities is not None
            else validate_process_ir(ir, symbols)
        )
        errors += len(report.errors)
        warnings += len(report.warnings)
        advisories += len(report.advisories)
        # #146 amendment: forward the validator's OWN message and remediation.
        #
        # This layer used to write "Semantic validation rejected this process."
        # and "Fix the reported node and re-plan." over the top of them. Both
        # are true and neither is actionable — the validator already knows that
        # a Branch leg depends on state a later leg writes, and already knows
        # the fix is to reorder the legs. Genericizing that at the public
        # boundary is what forced a caller back to repository knowledge.
        #
        # Safe to forward: every string is a STATIC table entry selected by
        # code, and the evidence is re-validated against a closed key allowlist
        # at this boundary. Nothing authored crosses.
        for severity, findings in (
            ("error", report.errors),
            ("warning", report.warnings),
            # Advisories were counted and DROPPED. An advisory that never
            # reaches the caller is an advisory nobody can act on, and its count
            # in the summary then describes something they cannot see.
            ("advisory", report.advisories),
        ):
            for finding in findings:
                codes.append(finding.code)
                diagnostics.append(
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        severity,
                        message=finding.message,
                        path=finding.path,
                        subject_kind="process",
                        subject_id=component_key,
                        remediation=finding.remediation,
                        cause_codes=(finding.code,),
                        node_identity=getattr(finding, "node_identity", "") or "",
                        evidence=_safe_evidence(finding),
                        authoring_contract_entry_ids=_contract_ids_for(finding.code),
                    )
                )

    # #153 (QA-153-r2-07): relocatability, decided HERE rather than only inside
    # the apply loop.
    #
    # The rule itself is unchanged and is not restated — `envelope_relocatability_
    # offenders` is the same authority the materialization plan's own validator
    # consults, so the two cannot drift. What changes is WHEN it fires. Deciding
    # it only at apply meant a caller learned their extension binding was
    # unusable after the connector components had already been written, and the
    # published `PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE` code was
    # unreachable from `plan` and `compile` entirely. The offending path is fully
    # decidable from the request, so nothing justifies charging a partial write
    # for the answer.
    for unit in normalized.integration_spec.processes:
        offenders = envelope_relocatability_offenders(unit.envelope, unit.process_ir)
        if not offenders:
            continue
        errors += len(offenders)
        for path in offenders:
            codes.append(PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE)
            diagnostics.append(
                _diag(
                    PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE,
                    "error",
                    message=(
                        "A process extension binding names a literal component "
                        "id; a materializable root may carry only '$ref:KEY' "
                        "tokens."
                    ),
                    path="/{0}".format(path),
                    subject_kind="process",
                    subject_id=unit.envelope.component_key,
                    remediation=(
                        "Declare the existing component in the component plan "
                        "and reference it by logical key with '$ref:KEY'."
                    ),
                    cause_codes=(PROCESS_MATERIALIZATION_REFERENCE_NOT_RELOCATABLE,),
                )
            )

    summary = ValidationReportSummaryV1(
        is_valid=errors == 0,
        error_count=errors,
        warning_count=warnings,
        advisory_count=advisories,
        codes=tuple(sorted(set(codes))),
    )
    return summary, tuple(diagnostics), symbols, resolution.capabilities_by_root


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

        # BOTH participant families (Codex round 1). Recipe process components
        # are lifted out of `components` and into `processes` by this slice, so
        # projecting only `components` regressed a topology relation naming a
        # composed process root from a valid symbol to TOPOLOGY_REFERENCE_NOT_FOUND.
        symbols = tuple(
            ComponentPlanSymbolV1(
                component_key=component.key, component_type=component.type
            )
            for component in normalized.integration_spec.components
        ) + tuple(
            ComponentPlanSymbolV1(
                component_key=unit.envelope.component_key, component_type="process"
            )
            for unit in (normalized.integration_spec.processes or ())
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
                cause_codes=_cause_codes_for(exc),
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

    payload = {
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
    # #154. The key is ABSENT unless the caller actually declared something.
    # Adding `"effect_declarations": None` unconditionally would change the
    # canonical payload of every request ever made, rotating every existing
    # semantic/plan/compile hash for a feature those requests do not use — and a
    # binding that rotates for no behavioural reason forces a re-plan that
    # establishes nothing.
    if request.effect_declarations is not None:
        payload["effect_declarations"] = request.effect_declarations.canonical_payload()
    return payload


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
    #: #153 §7 (AR1-01): the COMPILED materialization plan per process key —
    #: populated by compile, retained on the bundle, EXECUTED by apply. `None`
    #: before compile has run; an empty mapping means no canonical roots.
    materialization_plans: Optional[Mapping[str, Any]] = None
    #: #154: the per-root trusted effect context PLAN resolved. Compile reuses
    #: the resolution rather than redoing it, so the artifact cannot be compiled
    #: under a different context than the one validation reported against.
    effect_capabilities: Optional[Mapping[str, Any]] = None


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
    validation, validation_diagnostics, symbols, effect_capabilities = _validate_processes(
        normalized,
        request.effect_declarations,
        request.intent.conflict_policy,
        literal_indexes=_literal_profile_indexes(boomi_client, normalized),
    )
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
        # BLOCKING for every intent kind (#153).
        #
        # This carried an exemption — `intent_kind != "process_ir"` — justified by
        # "a direct ProcessIR intent is plan/compile-only by design, so nothing
        # would be built from it either way". That premise was true when #146
        # wrote it and is FALSE as of this milestone: a `process_ir` intent is
        # applied, its component plan IS built, and `_apply_plan` refuses on the
        # same `error_` prefix this lint reports. So the exemption downgraded a
        # guaranteed apply failure to a warning for precisely the intent kind
        # that had just become appliable, and compile would have issued a binding
        # whose apply could not succeed — the exact failure the error branch
        # exists to prevent.
        #
        # Deleted rather than re-conditioned: the condition that would replace it
        # is "will this plan be built?", and the answer is now unconditionally
        # yes. Found by reading the comment against the behaviour this slice
        # ships, not by a test — nothing failed, because the two tests covering
        # it both asserted the stale premise.
        unexecutable = tuple(
            _diag(
                AUTHORING_COMPILE_BLOCKED,
                "error",
                message=(
                    "The component-plan lint marked this step unexecutable; "
                    "apply would refuse it."
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
        errors = sort_authoring_diagnostics(errors + unexecutable)
        # The redacted echo REPLACES the caller's spec. This is the single line
        # that stops a plaintext password from riding back out in the preview.
        #
        # #153 (QA-153-r1-02): the echo is rebuilt from the NORMALIZED spec,
        # which since this slice carries `processes[]` — so this line restored
        # every authored ProcessIR body that
        # `build_integration_spec_preview` had just withheld, and the planted
        # canaries appeared in every served plan and compile response. The
        # withholding is therefore re-applied AFTER the overwrite rather than
        # only before it.
        #
        # Re-applied rather than moved: the legacy redaction above is what
        # protects the COMPONENT configs, and the root withholding is what
        # protects the process bodies. Both are needed, and the last write wins,
        # so the order is load-bearing. `_withhold_process_roots` is idempotent,
        # which is why calling it twice is safe rather than merely tolerable.
        if isinstance(legacy.get("integration_spec"), dict):
            spec_preview = _withhold_process_roots(
                IntegrationSpecV1(**legacy["integration_spec"])
            )
        # The planner's own warning strings. The advisory arm this used to
        # accumulate is gone with the `process_ir` exemption above: an
        # unexecutable step is now an ERROR for every intent kind, so there is no
        # advisory copy of it left to preserve.
        legacy_warnings = tuple(
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
        # The INTERNAL spec, never `spec_preview`. The preview withholds the
        # authored roots because it is served (see
        # `build_integration_spec_preview`), and reference resolution is an
        # internal computation over the real plan — feeding it the redacted
        # projection dropped every process participant and reported a `$ref` to
        # a declared root as DANGLING, which is the precise distinction this
        # summary exists to draw. Redaction belongs at the serving boundary, not
        # in the data the server reasons over.
        normalized.integration_spec,
        boomi_client=boomi_client,
        authored_refs=_iter_authored_refs(
            # `warnings=False` for the AR2-01 reason: these roots feed a ref scan,
            # and a mutated one would render its authored value into a warning.
            [
                ir.model_dump(mode="json", warnings=False)
                for _key, ir in normalized.process_roots
            ]
            + [normalized.integration_spec.model_dump(mode="json")]
        ),
    )
    # Surfaced at PLAN time, not first discovered at apply: a caller must be
    # able to learn about any remaining capability gap before spending a
    # compile. (Since #153 a direct ProcessIR intent is fully appliable; the
    # gap list is empty unless the registry says otherwise.)
    gaps = sort_by_key(
        normalized.gaps
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
        # The INTERNAL spec: the preview withholds the roots entirely, so a
        # summary built from it can only ever describe the components half.
        component_dependencies=build_component_dependencies(
            normalized.integration_spec
        ),
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
        effect_capabilities=effect_capabilities,
    )
    return result, internals


# ---------------------------------------------------------------------------
# phase 2 — read-only canonical compile
# ---------------------------------------------------------------------------


def _build_compile_time_plan(unit, symbols, conflict_policy, capabilities=None):
    """Build ONE root's materialization plan at COMPILE time, loudly.

    §6 review AR1-01: the previous version fingerprinted the plan and threw the
    OBJECT away ("Fingerprinted here and NOT stored") — so apply rebuilt a fresh
    plan whose self-check proved only that it matched itself, and a plan that
    failed to construct here silently produced a binding with NO plan artifact
    while apply might still build and execute one. Both halves are closed: the
    plan is RETAINED (threaded through `_PlanInternals` onto `CompiledBundle`,
    which apply consumes), and a construction failure is a compile refusal
    rather than a silent omission — everything this builder can reject is
    decidable from the request, so nothing justifies deferring the error to a
    phase that mutates.

    `conflict_policy` is the request's own (CX2-01). One divergence remains and
    is deliberate: a clone COLLISION renames the artifact at execution time as
    an attested overlay; the plan itself — and its fingerprint — stay exactly
    what compile certified (plan §3, clone-name exclusion).
    """
    from ..categories.integration_builder import _materializer_revision
    from ..compiler.process_ir.emitter_registry import emitter_revision
    from .contract import get_authoring_revisions
    from .process_materialization import build_materialization_plan

    try:
        return build_materialization_plan(
            envelope=unit.envelope,
            process_ir=unit.process_ir,
            symbols=symbols,
            conflict_policy=conflict_policy,
            capabilities=capabilities,
            compiler_revision=get_authoring_revisions()["compiler_revision"],
            emitter_revision=emitter_revision(),
            materializer_revision=_materializer_revision(),
        )
    except Exception as exc:  # noqa: BLE001 — surfaced, never swallowed
        raise AuthoringWorkflowError(
            AUTHORING_COMPILE_BLOCKED,
            (
                _diag(
                    AUTHORING_COMPILE_BLOCKED,
                    "error",
                    message=(
                        "The materialization plan for this root could not be "
                        "constructed, so no binding can certify what apply "
                        "would build."
                    ),
                    subject_kind="process",
                    subject_id=unit.envelope.component_key,
                    remediation=(
                        "Fix the reported condition and recompile; apply is "
                        "refused until the plan is constructible."
                    ),
                    cause_codes=_cause_codes_for(exc),
                ),
            ),
        ) from exc


def _cause_codes_for(exc) -> Tuple[str, ...]:
    """The cause codes a blocked-compile diagnostic should report for `exc`.

    QA round 16 (QA-153-r16-01): both sites below reported `type(exc).__name__`,
    which for a pydantic failure is the literal string `ValidationError` — so
    every registered code a model raises collapsed into one uninformative token
    on the served envelope. `PROCESS_MATERIALIZATION_PLAN_INVALID` and
    `PROCESS_MATERIALIZATION_FINGERPRINT_MISMATCH` are both declared codes with
    no reachable producer through this wrapper; relocatability escaped only
    because it happens to have a dedicated pre-check ahead of the model.

    So the code is ASKED of the validation rather than inferred from the Python
    class: `_named_error_code_from_validation` reads the errors' own registered
    types through `_NAMED_VALIDATION_CODES`, which is the authority for that
    mapping. It returns None for anything unrecognised, so the type name remains
    the answer for every failure that was not already named — this widens what
    is reported, never what is refused.
    """
    #154 (QA-154-r1-02) closed the SAME defect for a second exception type.
    # A `ProcessIRCompileError` carries `diagnostics`, and every one of those
    # rows already holds a registered code — so serving `"ProcessIRCompileError"`
    # threw away the authoritative answer the exception was carrying. That is the
    # same pair QA-153-r16-01 closed for pydantic (an exception CLASS treated as
    # provenance), recurring on a different class, so the rule is stated ONCE
    # here rather than patched per type: ASK the exception for its own codes,
    # in priority order, and fall back to the class name only when it has none.
    from ..categories.integration_builder import _named_error_code_from_validation

    named = _named_error_code_from_validation(exc)
    if named:
        return (named,)
    carried = tuple(
        code
        for code in (
            getattr(diagnostic, "code", None)
            for diagnostic in getattr(exc, "diagnostics", ()) or ()
        )
        if isinstance(code, str) and code
    )
    if carried:
        # Deduplicated, order preserved: a compile that failed on three nodes of
        # one kind should not report that kind three times.
        seen = {}
        for code in carried:
            seen.setdefault(code, None)
        return tuple(seen)
    return (type(exc).__name__,)


def build_artifact_descriptors(
    normalized: _NormalizedIntent,
    symbols: Any,
    conflict_policy: str = "reuse",
    effect_capabilities: Optional[Mapping[str, Any]] = None,
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
    from ..compiler.process_ir.pipeline import compile_process_ir_model_v1
    from ..models.process_ir import canonical_process_ir_json

    fingerprints: List[ArtifactFingerprintV1] = []
    cfg_summaries: List[ProcessCfgSummaryV1] = []
    materialization_plans: Dict[str, Any] = {}
    units_by_key = {
        unit.envelope.component_key: unit
        for unit in (normalized.integration_spec.processes or ())
    }

    for component_key, ir in normalized.process_roots:
        try:
            # Re-parse from the dump for the same reason the validator does: the
            # object may have been built by something other than a strict parse.
            # `warnings=False` for the AR2-01 reason: dumping a model that may
            # have been mutated renders the caller's values into a serializer
            # warning before the value-free parser runs.
            #
            # #178: hand over the MODEL, never a dump this call site chose for
            # itself. A `mode="json"` dump REPAIRS a wrong-typed value before the
            # parser sees it (a `datetime` in a `str` field becomes an ISO
            # string), so dumping here accepted mutated models that the compile
            # entry refuses — two public paths, two answers for one model. QA
            # round 2 measured it. The conversion now happens in exactly one
            # place, inside the compiler, and this site takes back the
            # re-validated model so it canonicalizes the thing actually compiled.
            #
            # It is also ONE call rather than a parse plus a compile: the compile
            # entry re-parses for itself, so the old pairing parsed twice on the
            # live authoring path. And it closes a real hole — the old parse
            # raised `ProcessIRValidationError`, which the `except` below does NOT
            # catch (only `ProcessIRCompileError`), so a re-parse failure escaped
            # this module's error channel raw.
            # #154: the SAME per-root context plan validated under. Passed only
            # when present, for the same fail-closed reason as the validate call.
            root_capabilities = (effect_capabilities or {}).get(component_key)
            reparsed, cfg, plan = (
                compile_process_ir_model_v1(ir, symbols, capabilities=root_capabilities)
                if root_capabilities is not None
                else compile_process_ir_model_v1(ir, symbols)
            )
        except ProcessIRCompileError as exc:
            raise AuthoringWorkflowError(
                AUTHORING_COMPILE_BLOCKED,
                tuple(
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        message=_compile_message(diagnostic),
                        path=getattr(diagnostic, "path", "") or "",
                        subject_kind="process",
                        subject_id=component_key,
                        # NOT "re-plan". Plan reports this input valid — reference
                        # resolution is not one of the semantic-validation phases
                        # — so telling a caller to re-plan sent them round a loop
                        # they could not exit (issue #146 QA, bug #409). The
                        # failure is usually in the COMPONENT the reported node
                        # references, not in the node as authored.
                        # #146 amendment: the compiler's OWN remediation for this
                        # code, with the reference-resolution note appended
                        # rather than replacing it. The note is what bug #409
                        # needed — plan reports this input valid, so "re-plan"
                        # sent callers round a loop they could not exit — and the
                        # per-code text is what tells them WHICH thing to fix.
                        remediation=_compile_remediation(diagnostic),
                        cause_codes=(getattr(diagnostic, "code", "") or "",),
                        node_identity=getattr(diagnostic, "node_identity", "") or "",
                        authoring_contract_entry_ids=_contract_ids_for(
                            getattr(diagnostic, "code", "") or ""
                        ),
                    )
                    for diagnostic in (exc.diagnostics or ())
                )
                or (
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        # The ONLY place the generic headline is correct: the
                        # compiler raised without naming a single diagnostic, so
                        # there is no authority message to forward.
                        message=_COMPILE_GENERIC_MESSAGE,
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

        # ...and the RELOCATABLE MATERIALIZATION PLAN, so the compile binding
        # covers it (Codex round 1).
        #
        # The plan was first constructed inside apply, and these descriptors
        # carried only the normalized IR and the emission plan — so the caller's
        # compile hash did not cover the materializer revision, the emitter
        # revision, or the preservation policy. Change any of those and every
        # previously-issued binding still verified: preflight accepted it, apply
        # built a NEW plan, and the plan self-validated against its own
        # fingerprint. A binding that cannot notice the artifact changing is not
        # a staleness check.
        #
        # Fingerprinted AND retained (§6 AR1-01). An earlier draft rebuilt the
        # plan at apply and compared only the digest; retention is what makes
        # apply execute the very plan this compile fingerprinted rather than a
        # look-alike rebuilt from the same inputs.
        #
        # QA-180-r1-03: this comment used to say the plan is "NOT stored ...
        # rebuilt at apply", three lines above the assignment that stores it.
        # It is the same stale-claim mechanism #180 records as SELF-180-02,
        # pointed at retention instead of at the compile count.
        unit = units_by_key.get(component_key)
        if unit is not None:
            materialization_plan = _build_compile_time_plan(
                unit, symbols, conflict_policy,
                (effect_capabilities or {}).get(component_key),
            )
            materialization_plans[component_key] = materialization_plan
            fingerprints.append(
                ArtifactFingerprintV1(
                    component_key=component_key,
                    component_type="process",
                    artifact_kind="process_component_materialization_plan",
                    byte_length=len(
                        canonical_plan_material(materialization_plan)
                    ),
                    digest=materialization_plan.plan_fingerprint,
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

    return sort_by_key(fingerprints), sort_by_key(cfg_summaries), materialization_plans


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

    fingerprints, cfg_summaries, materialization_plans = build_artifact_descriptors(
        internals.normalized,
        internals.symbols,
        request.intent.conflict_policy,
        internals.effect_capabilities,
    )
    # RETAINED on the internals (AR1-01), so the bundle — and therefore apply —
    # executes the very plans this compile fingerprinted, never a rebuild.
    internals = replace(internals, materialization_plans=materialization_plans)

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

    ``integration_spec`` is the INTERNAL spec — the one apply builds from. It is
    emphatically not ``compile_result.integration_spec_preview``, and the
    invariant below is what enforces that rather than trusting the caller to
    remember it.
    """

    integration_spec: IntegrationSpecV1
    compile_result: AuthoringCompileResultV1
    request: AuthoringRequestV1
    #: #153 §7 (AR1-01): the compiled materialization plan per process key.
    #: Apply EXECUTES these; it never rebuilds. Enforced in __post_init__.
    materialization_plans: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Refuse a bundle whose spec cannot build what the compile describes.

        QA-153-r2-01: this bundle was constructed from
        ``compile_result.integration_spec_preview`` — the SERVED projection,
        which withholds the authored process roots because it is echoed back to
        the caller. The bundle's spec is also the apply INPUT, so every canonical
        root was silently deleted on its way to the builder: apply reported
        ``_success: true``, ``mutation_status: "performed"``, "Applied … with 2
        steps", and created no process component, with no warning naming the
        root it dropped. A silent no-op that attests a successful mutation is
        worse than the loud crash it replaced.

        The check compares against ``process_cfg``, not against the request.
        ``process_cfg`` is derived from the internal spec by compilation and is
        SERVED (it is a value-free summary, so it is never withheld), which makes
        it an authority independent of the projection under test. Any served spec
        projection therefore fails here for every intent that declares a root,
        at construction, instead of at the mutation.
        """
        described = {summary.component_key for summary in self.compile_result.process_cfg}
        carried = {
            unit.envelope.component_key for unit in self.integration_spec.processes
        }
        # ...and every described root must carry its COMPILED plan (AR1-01): a
        # bundle without one would send apply back to rebuilding, which is the
        # self-certification hole this field exists to close.
        missing = sorted(
            (described - carried)
            | (described - set(self.materialization_plans or {}))
        )
        if missing:
            raise AuthoringWorkflowError(
                AUTHORING_COMPILE_BLOCKED,
                tuple(
                    _diag(
                        AUTHORING_COMPILE_BLOCKED,
                        "error",
                        message=(
                            "The compiled bundle does not carry a process root "
                            "its own compile describes; nothing was mutated."
                        ),
                        subject_kind="process",
                        subject_id=key,
                        remediation=(
                            "This is a server defect: apply was handed a served "
                            "spec projection instead of the internal spec."
                        ),
                    )
                    for key in missing
                ),
            )


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
    # #146: the SAME parser as plan and compile, so a malformed ProcessIR is
    # reported identically at all three sites rather than depending on which one
    # the caller reached.
    request = parse_authoring_request_v1(dict(raw_request_payload))

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
        compile_result, internals = compile_authoring_request_v1(
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
    # binding minted against account A cannot satisfy an apply against account B.
    #
    # It is NOT a per-profile boundary: two profiles addressing one account share
    # a scope hash, so a compile made under one applies under the other. That is
    # deliberate and recorded (AUTHORING_WORKFLOW_V1.md §11) — the binding is a
    # staleness check, not an authorization token, and Boomi's own authorization
    # still governs what each credential may do.

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
    # #153 (M12.15): the process-materialization refusal is GONE.
    #
    # It said "no production path materializes a ProcessIR root", which was true
    # when it was written and is now false: the canonical chain compiles the root,
    # binds real ids in topological order, materializes through the neutral
    # ProcessComponentMaterializer and records both attestations. Keeping the
    # refusal would refuse the capability this milestone exists to ship.
    #
    # Deleted rather than made conditional. A conditional refusal would need a
    # second predicate for "is this root materializable", and that predicate is
    # exactly the compiled artifact the plan already describes — a second
    # authority answering a question the first one already answered.

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
                        "binding minted against a DIFFERENT ACCOUNT also fails "
                        "here, because the account scope is part of the hash "
                        "chain. Two profiles addressing the SAME account share a "
                        "scope and do not conflict."
                    ),
                )
                for field in sorted(mismatches)
            ),
        )

    return CompiledBundle(
        # The INTERNAL spec, never the preview — see `CompiledBundle.__post_init__`
        # and QA-153-r2-01. The same distinction the reference summary already
        # draws at `build_resolved_reference_summary`: redaction and withholding
        # belong at the serving boundary, not in the data the server acts on.
        integration_spec=internals.normalized.integration_spec,
        compile_result=compile_result,
        request=request,
        materialization_plans=dict(internals.materialization_plans or {}),
    )


# ---------------------------------------------------------------------------
# verify-side provenance comparison
# ---------------------------------------------------------------------------


#: The revisions a verify compares. Named once: the loop and the "was this
#: comparison even complete?" fallback must agree, and they did not.
_COMPARED_REVISIONS: Tuple[str, ...] = ("capability_revision", "compiler_revision")


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
    # #146 amendment: BOTH revisions, and the report says which moved.
    #
    # Comparing only `capability_revision` meant a server whose compiler
    # BEHAVIOUR had changed — a placement rule, a replay classification, a
    # remediation — reported `match` against a binding made before the change.
    # That is the §11 limit this amendment claims to close, and claiming it
    # closed while comparing one of two would have been the same false-clean
    # this surface exists to prevent.
    revision_mismatches: List[str] = []
    if binding:
        for field in _COMPARED_REVISIONS:
            expected = binding.get(field)
            # A binding minted before the field existed carries no value; that
            # is unknown, not a mismatch, and is reported as such below.
            if expected is not None and expected != revisions[field]:
                revision_mismatches.append(field)
        # PRECEDENCE, stated because it was previously only implicit: a
        # definite mismatch outranks an absent field. "Something you can act on
        # is wrong" is more useful than "part of this was not checked" — but
        # the two are no longer conflated, because `revision_uncompared`
        # reports the absent fields alongside the mismatch instead of leaving a
        # short mismatch list to be misread as "the rest matched".
        if revision_mismatches:
            skew = "mismatch"
        elif any(binding.get(field) is None for field in _COMPARED_REVISIONS):
            # A binding missing EITHER field was never fully compared, so
            # "match" would claim a comparison that did not happen. Keying the
            # fallback on `capability_revision` alone contradicted this code's
            # own comment and reported `match` for a binding carrying no
            # compiler revision at all.
            skew = "unknown"
        else:
            skew = "match"
    else:
        skew = "unknown"
    revision_mismatches = sorted(revision_mismatches)
    # `binding and ...` made the ONE case where NOTHING was compared report an
    # empty list — indistinguishable, on this field, from a binding where both
    # revisions were compared and agreed. An absent binding does not compare
    # fewer fields than a partial one; it compares none.
    revision_uncompared = sorted(
        field
        for field in _COMPARED_REVISIONS
        if not binding or binding.get(field) is None
    )

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
        revision_uncompared=tuple(revision_uncompared),
        revision_mismatches=tuple(revision_mismatches),
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
