"""Strict (``version="1.1"``) top-level pipeline-authority evaluation — ADR-001 §5.

Issue #139D. ``IntegrationSpecV1.pipeline`` is an authored-but-INERT analysis view:
nothing consults it, while a process component's own config is what actually
executes. The two surfaces can therefore disagree, and today the executable one
wins **silently** (ADR-001 §5, frozen — not endorsed — by the #135
characterization suite).

ADR-001 §5 replaces that silence with exactly two outcomes: *derived equality*, or
a stable ``LEGACY_ADAPTER_AUTHORITY_CONFLICT`` rejection. Precedence-based
reconciliation between authored duplicates is permanently rejected. Because
hard-rejecting a payload that plans clean today would be an unannounced
compatibility break, §9 puts the strictness on a **new opt-in surface**:

* ``version="1.0"`` (default, and the only value any archetype emits) — frozen.
  Every payload accepted today stays accepted, and the authored pipeline stays a
  preserved inert echo. This module returns :data:`NOT_APPLICABLE` for it and the
  caller changes nothing.
* ``version="1.1"`` — strict. A disagreeing or ambiguous authored pipeline is
  rejected at **plan time, before collision resolution and before any mutation**.

The selector is a new ``version`` literal rather than an optional field because
``IntegrationSpecV1`` does not set ``model_config``, so pydantic's default
``extra="ignore"`` would silently DROP an unknown opt-in key and degrade the
request to legacy precedence — the exact failure §5 requires the selector to be
immune to. A pre-#139 server validating ``version="1.1"`` against its
``Literal["1.0"]`` raises instead, which is the required fail-closed behaviour.

Account independence (ADR-001 §5, "Determinism note"). Every decision here is
computed from the **authored payload alone**. This module never reads live account
contents, and the accept-vs-reject outcome can therefore never flip on what
happens to exist in the target account. Only the *representation* of an accepted
inert view may follow materialization — that is the caller's post-collision
view-faithfulness hook, never a decision made here.

Secrets (ADR-001 §7/§11). The normalized comparison form built here is ephemeral:
it is never logged, never returned, and never embedded in a diagnostic. The
authority errors the caller raises from these dispositions are deliberately
value-free.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

__all__ = [
    "STRICT_VERSION",
    "LEGACY_VERSION",
    "NOT_APPLICABLE",
    "PRESERVE_INERT",
    "UNDECIDABLE",
    "AGREE",
    "AMBIGUOUS",
    "DISAGREE",
    "NOT_REPRESENTABLE",
    "AuthorityDecision",
    "evaluate_pipeline_authority",
]

LEGACY_VERSION = "1.0"
STRICT_VERSION = "1.1"

#: The spec is not on the strict surface, or authors no top-level pipeline.
NOT_APPLICABLE = "not_applicable"
#: Zero authored process components — the authored view is preserved inert.
PRESERVE_INERT = "preserve_inert"
#: The submitted process has no comparable semantics (ADR-001 §5 clean-plan gate).
UNDECIDABLE = "undecidable"
#: The authored view equals the submitted process's normalized semantics.
AGREE = "agree"
#: Two or more authored processes — no marker says which one the view summarizes.
AMBIGUOUS = "ambiguous"
#: The authored view contradicts the submitted process's normalized semantics.
DISAGREE = "disagree"
#: One side is VALID but has no representation in the singular linear view, so the
#: two can never be made to agree. Distinguished from :data:`DISAGREE` purely for
#: diagnostics: telling such a caller to "make the view match" is unsatisfiable
#: advice, because no value of the authored pipeline would ever match.
NOT_REPRESENTABLE = "not_representable"


@dataclass(frozen=True)
class AuthorityDecision:
    """The outcome of one strict-surface authority evaluation.

    ``authored_key`` names the single authored process component when there is
    exactly one, so the caller can find its step for the post-collision
    view-faithfulness hook. It is deliberately the ONLY payload-derived value
    carried out of this module, it is the caller's own component key (already
    echoed in every plan step), and it never reaches a diagnostic.
    """

    disposition: str
    authored_key: Optional[str] = None


# ---------------------------------------------------------------------------
# The comparison normal form
# ---------------------------------------------------------------------------
# ``SyncPipelineBuilder.lower_config`` normalizes a PipelineSpec into an
# equivalent ``database_to_api_sync`` config -- PipelineSpec -> block config, not
# the reverse. No config -> PipelineSpec derivation exists anywhere in the tree,
# so the lowered block config is the only common form both surfaces can reach
# without inventing a second semantic compiler (which §6 forbids).
#
# That fixes the DOMAIN of this comparison precisely: the normal form is the
# IMAGE of ``lower_config``. A submitted config carrying anything lowering could
# never produce is, by construction, outside the linear PipelineSpec view -- so
# it is "valid but not representable", which §5 treats as a disagreement rather
# than as agreement-by-omission. This makes the check FAIL-CLOSED: a connector
# feature added to ``database_to_api_sync`` later falls outside the image
# automatically and yields a conflict, never a false agreement.
_CORE_KEYS = ("process_kind", "source", "transform", "target")

# Non-flow envelope metadata. ``lower_config`` carries description /
# process_extensions through verbatim, and the integration builder injects
# name/component metadata into the build payload. None of it changes the flow, so
# it is ignored rather than treated as an unrepresentable feature.
# ``process_type`` is listed here because it is a KIND SELECTOR, not a flow
# feature: :func:`_resolve_process_kind` reads it, and the resolved kind is what
# lands in the normal form. It must never be silently ignored the way the rest of
# these genuinely inert keys are -- see that function.
_INERT_TOP_LEVEL_KEYS = frozenset(
    {
        "description",
        "process_extensions",
        "folder_name",
        "name",
        "process_type",
        "component_type",
        "component_name",
    }
)

# The exact key sets ``_lower_binding_stage`` / ``_lower_map_stage`` can emit.
# A gated sub-block (dynamic_path, reliability, runtime_bindings, ...) is outside
# them, so it makes the submitted process unrepresentable.
_BINDING_KEYS = frozenset(
    {"connector_type", "action_type", "connection_id", "operation_id", "label"}
)
_TRANSFORM_KEYS = frozenset({"mode", "map_ref", "map_id", "label"})

# Sentinels distinguishing "no clean semantics to compare" (the clean-plan gate,
# whose own validation error must surface untouched) from "valid, but this
# process is categorically outside the singular linear view" (a disagreement).
_UNDECIDABLE_CORE = object()
_UNREPRESENTABLE_CORE = object()


def _norm_text(value: Any) -> str:
    """Absent / null / blank all collapse to one comparable empty string."""
    if not isinstance(value, str):
        return ""
    return value.strip()


def _canonical_binding(binding: Any) -> Optional[Dict[str, Any]]:
    """Canonicalize one source/target connector binding, or None if unrepresentable."""
    if not isinstance(binding, dict):
        return None
    if set(binding) - _BINDING_KEYS:
        return None
    # Canonicalize connector_type/action_type through the COMPILER'S OWN
    # family-conditional rule rather than a local one, so the comparison agrees
    # with what is actually emitted by construction. The rule is not uniform:
    # the legacy linear builder UPPER-cases a REST verb (so `post` and `POST`
    # emit identical XML) but preserves a database verb (so `Get` and `get` do
    # NOT), and it LOWER-cases a non-REST connector type (so `Database` and
    # `database` emit identically). Comparing raw stripped spellings therefore
    # manufactured false conflicts in two directions at once; #139C's helper
    # already encodes exactly this rule and is pinned against the legacy builder.
    from ..lowering import _canonical_connector_metadata

    connector, action = _canonical_connector_metadata(
        "source", binding.get("connector_type") or "", binding.get("action_type") or ""
    )
    return {
        "connector_type": connector,
        "action_type": action,
        "connection_id": _norm_text(binding.get("connection_id")),
        "operation_id": _norm_text(binding.get("operation_id")),
        "label": _norm_text(binding.get("label")),
    }


def _canonical_transform(transform: Any) -> Optional[Dict[str, Any]]:
    """Canonicalize a transform block, or None if unrepresentable."""
    if transform is None:
        return {"mode": "passthrough", "map_ref": "", "label": ""}
    if not isinstance(transform, dict):
        return None
    if set(transform) - _TRANSFORM_KEYS:
        return None
    mode = _norm_text(transform.get("mode")).lower() or "passthrough"
    if mode not in ("passthrough", "map_ref"):
        return None
    # Mirror _lower_map_stage's own `map_ref or map_id` precedence exactly so the
    # two spellings of one selector never read as a conflict.
    map_ref = "" if mode == "passthrough" else _norm_text(
        transform.get("map_ref") or transform.get("map_id")
    )
    return {"mode": mode, "map_ref": map_ref, "label": _norm_text(transform.get("label"))}


def _resolve_process_kind(config: Dict[str, Any]) -> str:
    """Resolve the process kind, honouring the supported ``process_type`` alias.

    Every other layer resolves ``process_kind or process_type`` — the plan-time
    gate (``integration_builder._build_plan``) and each process builder's
    ``validate_config``/``build``. Reading only ``process_kind`` here would let a
    caller opt in to the strict surface, author a contradictory top-level
    pipeline, spell the kind ``process_type``, and have this check fall through to
    ``UNDECIDABLE`` while the process still built normally — a silent bypass of
    the whole guarantee.
    """
    return _norm_text(config.get("process_kind") or config.get("process_type"))


def _canonical_core(lowered: Dict[str, Any]) -> Any:
    """Project a lowered block config onto the comparison normal form."""
    source = _canonical_binding(lowered.get("source"))
    target = _canonical_binding(lowered.get("target"))
    transform = _canonical_transform(lowered.get("transform"))
    if source is None or target is None or transform is None:
        return _UNREPRESENTABLE_CORE
    return {
        # The RESOLVED kind, so a config spelling it `process_type` compares equal
        # to the authored view's lowered `process_kind` instead of reading as "".
        "process_kind": _resolve_process_kind(lowered),
        "source": source,
        "transform": transform,
        "target": target,
    }


def _core_from_authored_pipeline(pipeline: Any) -> Any:
    """Normalize the authored top-level PipelineSpec through the real lowering.

    A schema-valid PipelineSpec that cannot lower (a reserved stage kind, a
    component_ref stage, a non-linear graph) has no representation in the
    executable normal form, so it can never equal the submitted process. Unlike
    the submitted side there is no error of its own to surface -- the authored
    view is inert on V1 -- so this is a disagreement, not the clean-plan gate.
    """
    from ....categories.components.builders.process_flow_builder import (
        BuilderValidationError,
        SyncPipelineBuilder,
    )

    try:
        lowered = SyncPipelineBuilder.lower_config(
            {"process_kind": SyncPipelineBuilder.PROCESS_KIND, "pipeline": pipeline.model_dump()}
        )
    except BuilderValidationError:
        return _UNREPRESENTABLE_CORE
    except Exception:  # pragma: no cover - defensive; lowering is total on dicts
        return _UNREPRESENTABLE_CORE
    return _canonical_core(lowered)


def _core_from_submitted_config(config: Any, depends_on: Any) -> Any:
    """Normalize a submitted process config, or return one of the two sentinels."""
    from ....categories.components.builders.process_flow_builder import (
        BuilderValidationError,
        ProcessFlowBuilder,
        SyncPipelineBuilder,
        WrapperSubprocessBuilder,
        _reliability_requests_try_catch,
    )

    if not isinstance(config, dict):
        return _UNDECIDABLE_CORE
    kind = _resolve_process_kind(config)

    if kind == SyncPipelineBuilder.PROCESS_KIND:
        # An invalid sync_pipeline has no clean semantics AND owns a real error
        # that must reach the caller unchanged, so it is the clean-plan gate
        # rather than a conflict.
        #
        # validate_config, NOT just lower_config. Lowering catches only structural
        # defects; a config can lower cleanly and still be rejected afterwards --
        # an undeclared `$ref` (MISSING_PROCESS_DEPENDENCY) or an unsupported REST
        # verb (PROCESS_CONNECTOR_BINDING_INVALID) both lower fine. Comparing such
        # a config would let an authority conflict MASK the actionable validation
        # error the caller actually needs. Mirrors the database_to_api_sync branch
        # below: validate first, normalize second.
        if (
            SyncPipelineBuilder.validate_config(config, depends_on=depends_on)
            is not None
        ):
            return _UNDECIDABLE_CORE
        try:
            lowered = SyncPipelineBuilder.lower_config(config)
        except BuilderValidationError:
            return _UNDECIDABLE_CORE
        except Exception:  # pragma: no cover - defensive
            return _UNDECIDABLE_CORE
        return _canonical_core(lowered)

    if kind == ProcessFlowBuilder.PROCESS_KIND:
        if ProcessFlowBuilder.validate_config(config, depends_on=depends_on) is not None:
            return _UNDECIDABLE_CORE
        # Valid. Representable only if it carries nothing outside the image of
        # lowering -- any flow_sequence / branch / return-documents block puts it
        # categorically outside the singular linear view.
        extras = set(config) - set(_CORE_KEYS) - _INERT_TOP_LEVEL_KEYS
        # ...with one measured exception. A `reliability` block that emits NO
        # Try/Catch wrapper changes no emitted byte -- `{retry_count: 0, dlq:
        # {mode: "disabled"}}` is the database_to_api_sync archetype's DEFAULT
        # shape, so treating its mere presence as a feature would lock the
        # flagship archetype's own output out of the strict surface for no
        # semantic reason. Decided by the builder's OWN predicate rather than a
        # re-implemented rule here, so the two can never drift (the Branch v1
        # composition guard uses the same one).
        #
        # Sound only AFTER validate_config passes, above: the predicate also
        # returns False for gated shapes (retry_count > 0 with no supported DLQ
        # mode and no catch_exception), and those are rejected by validation
        # before this point, so a surviving False here means genuinely inert.
        #
        # Scope, precisely: unknown keys are fail-closed at the TOP level of the
        # config (a future sibling block is non-representable automatically), but
        # INSIDE `reliability` the rule is "inert iff no Try/Catch is emitted",
        # not "unknown keys fail closed" -- `reliability: {"bogus_key": 1}` is
        # accepted, correctly, because it emits nothing. A future catch-EMITTING
        # reliability key therefore has to be taught to the predicate below; using
        # the emitter's own predicate is what guarantees it cannot start emitting
        # a Try/Catch without this check learning about it at the same time.
        if "reliability" in extras and not _reliability_requests_try_catch(
            config.get("reliability")
        ):
            extras.discard("reliability")
        if extras:
            return _UNREPRESENTABLE_CORE
        return _canonical_core(config)

    if kind == WrapperSubprocessBuilder.PROCESS_KIND:
        # A valid wrapper (start -> calls -> stop/return) is categorically not a
        # linear source/transform/target pipeline, so an authored top-level
        # pipeline describing one is a disagreement (ADR-001 §5: the absence of a
        # nested config.pipeline is NOT agreement).
        if WrapperSubprocessBuilder.validate_config(config, depends_on=depends_on) is not None:
            return _UNDECIDABLE_CORE
        return _UNREPRESENTABLE_CORE

    # Missing or unrecognized process_kind: PROCESS_KIND_REQUIRED (or the kind's
    # own error) owns this payload. No authority disposition.
    return _UNDECIDABLE_CORE


def _is_authoring_process(component: Any) -> bool:
    """Declared-authoring test, counted BEFORE collision resolution (ADR-001 §5).

    ``action`` is ``Literal["create", "update"]`` -- the only authorable actions.
    An ``update`` ALWAYS authors (it re-emits the process XML from its config),
    even with ``reference_only=true``, which the planner honours only for
    ``create``. Only a ``create`` flagged ``reference_only`` is excluded, which is
    this model's concrete representation of a pure reference.

    Both exclusions are properties of the caller's declaration, never of live
    account state -- that is what makes the ambiguity decision account-independent.
    """
    if _norm_text(getattr(component, "type", None)).lower() != "process":
        return False
    config = getattr(component, "config", None)
    reference_only = isinstance(config, dict) and bool(config.get("reference_only"))
    return not (getattr(component, "action", None) == "create" and reference_only)


def evaluate_pipeline_authority(spec: Any) -> AuthorityDecision:
    """Decide the strict-surface disposition of an authored top-level pipeline.

    Pure: reads only the authored spec, mutates nothing, performs no I/O.
    """
    if getattr(spec, "version", LEGACY_VERSION) != STRICT_VERSION:
        return AuthorityDecision(NOT_APPLICABLE)
    if getattr(spec, "pipeline", None) is None:
        return AuthorityDecision(NOT_APPLICABLE)

    authored = [c for c in spec.components if _is_authoring_process(c)]

    # Ambiguity is STRUCTURAL: it needs only the declared cardinality, so it
    # stands even when a process's semantics are unavailable. It is therefore
    # decided before -- and wins over -- the per-process clean-plan gate.
    if len(authored) >= 2:
        return AuthorityDecision(AMBIGUOUS)
    if not authored:
        return AuthorityDecision(PRESERVE_INERT)

    component = authored[0]
    submitted = _core_from_submitted_config(component.config, component.depends_on)
    if submitted is _UNDECIDABLE_CORE:
        return AuthorityDecision(UNDECIDABLE, component.key)

    authored_core = _core_from_authored_pipeline(spec.pipeline)
    # NOT_REPRESENTABLE is a property of the PROCESS ONLY. A caller whose process
    # is valid yet has no linear view (a wrapper_subprocess, a flow_sequence, a
    # wired Try/Catch + DLQ path) can never satisfy "make the view match", so
    # telling them to is unsatisfiable advice — their only correct action is to
    # drop the view.
    if submitted is _UNREPRESENTABLE_CORE:
        return AuthorityDecision(NOT_REPRESENTABLE, component.key)
    # The mirror case is NOT the same thing. Here the process DOES have a linear
    # view and only the authored view fails to lower (a `branch`/`decision`/
    # `listener`/`write` stage kind, an empty stage list). Blaming the process
    # would be false, and "author it as sync_pipeline" is advice the caller has
    # often already followed. It is an ordinary mismatch: the view is wrong, and
    # correcting it is exactly the achievable remedy DISAGREE already prescribes.
    if authored_core is _UNREPRESENTABLE_CORE:
        return AuthorityDecision(DISAGREE, component.key)
    if authored_core == submitted:
        return AuthorityDecision(AGREE, component.key)
    return AuthorityDecision(DISAGREE, component.key)
