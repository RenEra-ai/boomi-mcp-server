"""Server-side resolution of caller effect declarations (#154 M12.16).

THE ONE IDEA
------------
A caller may DECLARE what a map, script or child process does to process state.
That declaration is never trusted for its CONTENT. It is used for exactly two
things:

1. IDENTITY — which artifact is this declaration about? Answered by resolving the
   ref against the symbol table and, for scripts, by RECOMPUTING the digest from
   the resolved source. A declaration that names nothing in the request, resolves
   to the wrong component type, or carries a digest that does not match is
   rejected.
2. AGREEMENT — does the caller's claim match what the server derived on its own?
   A mismatch is rejected. An agreement adds nothing the derivation did not
   already establish; it just means the caller and the server say the same thing.

Effect CONTENT therefore comes only from a server-side authority:

* ``map_effects``        -- inspection of the resolved map component's function
                            mappings, using the map-function registry's effect
                            metadata.
* ``script_effects``     -- the server-owned vetted-script registry, keyed by the
                            RECOMPUTED ``(language, digest)``.
* ``subprocess_effects`` -- inspection of the child's authored ProcessIR, reusing
                            the lineage analysis's own ``_reads_of``/``_writes_of``
                            so there is no second model of what state is.
* ``external_writers``   -- NOTHING. An outside writer is not in the artifact.
                            The declaration is carried through as a bounded
                            assumption and can never establish cache state.

A declaration that passes identity but has no server-side authority behind its
content is INERT: it is dropped, and every strict finding it might have silenced
still fires. Inert is not an error — an unregistered script is a perfectly legal
thing to author, it just proves nothing.

CAPABILITIES ARE PER ROOT
-------------------------
Each root gets a FRESH capabilities object holding only the contracts that bind
inside that root. One shared object would carry contracts that bind nowhere in a
given root, which the compiler reports as an unbound contract — an error
manufactured by the plumbing rather than by the payload.

SECRETS
-------
Component configs enter this module. Nothing from them leaves it: every finding
below carries a fixed code and a JSON pointer built from indexes, never an
authored name, ref, digest, script body or property value.
"""

from __future__ import annotations

import re
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Sequence, Tuple

from ..errors import PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID

__all__ = [
    "EffectAuthorityFindingV1",
    "EffectResolutionV1",
    "resolve_process_ir_effect_declarations",
    "effect_authority_rows",
]

_INVALID = PROCESS_IR_CAPABILITY_EFFECT_CONTRACT_INVALID

#: A child ProcessIR's ROOT-sequence step path. The same shape the compiler's
#: entry invariant uses, and for the same reason: it separates the spine from a
#: control body.
_ROOT_STEP = re.compile(r"/body/steps/\d+")


class EffectAuthorityFindingV1:
    """A value-free rejection: code, JSON pointer, and a fixed reason token."""

    __slots__ = ("code", "path", "reason")

    def __init__(self, code: str, path: str, reason: str) -> None:
        self.code = code
        self.path = path
        self.reason = reason

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return "EffectAuthorityFindingV1({0!r}, {1!r}, {2!r})".format(
            self.code, self.path, self.reason
        )

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, EffectAuthorityFindingV1)
            and (self.code, self.path, self.reason) == (other.code, other.path, other.reason)
        )


class EffectResolutionV1:
    """Per-root trusted context plus every rejection encountered building it."""

    __slots__ = ("capabilities_by_root", "findings", "inert")

    def __init__(self, capabilities_by_root, findings, inert) -> None:
        self.capabilities_by_root = capabilities_by_root
        self.findings = tuple(findings)
        #: Declarations that passed identity but had no content authority. Kept
        #: so a caller can be told the difference between "rejected" and
        #: "accepted but establishes nothing".
        self.inert = tuple(inert)

    @property
    def ok(self) -> bool:
        return not self.findings


# ---------------------------------------------------------------------------
# occurrence walk — which refs actually appear in which root
# ---------------------------------------------------------------------------


def _iter_nodes(node: Any):
    """Every authored node reachable from a root, control bodies included."""
    yield node
    for attr in ("steps",):
        for child in getattr(node, attr, ()) or ():
            for item in _iter_nodes(child):
                yield item
    for attr in ("legs",):
        for leg in getattr(node, attr, ()) or ():
            for item in _iter_nodes(leg):
                yield item
    for attr in ("true_arm", "false_arm", "try_body", "catch_body", "body", "terminal"):
        child = getattr(node, attr, None)
        if child is not None and hasattr(child, "__class__") and not isinstance(child, (str, bytes)):
            for item in _iter_nodes(child):
                yield item


def _occurrences(ir: Any) -> Dict[str, set]:
    """Refs this root actually mentions, bucketed by the slot that mentions them."""
    found: Dict[str, set] = {
        "map_ref": set(),
        "process_ref": set(),
        "cache_get_ref": set(),
        "external_writer_ref": set(),
        "scripts": set(),
    }
    for node in _iter_nodes(ir.body):
        kind = getattr(node, "kind", None)
        if kind == "map_ref":
            found["map_ref"].add(node.map_ref)
        elif kind == "process_call":
            found["process_ref"].add(node.process_ref)
        elif kind == "cache_get":
            found["cache_get_ref"].add(node.cache_ref)
            if getattr(node, "external_writer", False):
                found["external_writer_ref"].add(node.cache_ref)
        elif kind == "data_process":
            for step in getattr(node, "steps", ()) or ():
                if getattr(step, "operation", None) == "custom_scripting":
                    found["scripts"].add((step.language, step.script))
    return found


# ---------------------------------------------------------------------------
# content authorities
# ---------------------------------------------------------------------------


def _symbol(symbols: Any, ref: str):
    for symbol in getattr(symbols, "symbols", ()) or ():
        if symbol.ref == ref:
            return symbol
    return None


def _may_be_substituted(spec: Any, conflict_policy: str) -> bool:
    """Whether the plan may bind an EXISTING artifact instead of this config.

    Effects must describe the artifact that will EXECUTE, so a component the plan
    RESOLVES rather than writes is opaque: its live content has not been read and
    is not version-bound.

    ``component_materialization_mode`` is the authority for which of those a spec
    is, and it is ASKED rather than re-derived. The first version of this function
    re-implemented it as "update is safe, otherwise depends on the policy" and
    drifted immediately: it missed ``reference_only``, which that function checks
    BEFORE ``action`` and which resolves to a reuse **independent of
    conflict_policy** (``integration_builder`` says so in as many words). So a
    ``{reference_only: true, map_type: "direct"}`` spec derived a pure,
    replay-safe effect for a component nobody had read.

    The policy overlay is the one thing that function does not model: a plain
    ``create`` may still COLLIDE and be reused, and only the request's
    ``conflict_policy`` decides that. ``clone`` writes a suffixed new component
    and ``fail`` refuses, so both leave the config authoritative.
    """
    # The module's OWN constants, imported rather than re-typed. The first
    # attempt at this fix compared against the literal "reuse" while the constant
    # is "reuse_reference", so it matched nothing and changed nothing — a
    # hand-copied vocabulary failing exactly the way the hand-copied RULE just
    # had. Two spellings of one value is the same defect at a smaller scale.
    from ..recipes.materialization import (
        _REUSE,
        _UPDATE,
        component_materialization_mode,
    )

    mode = component_materialization_mode(spec)
    if mode == _REUSE:
        return True
    if mode == _UPDATE:
        return False
    return conflict_policy == "reuse"


def _component(components: Sequence[Any], ref: str):
    """The authored spec for a ``$ref:key`` token, if this request carries one."""
    key = ref[len("$ref:"):] if ref.startswith("$ref:") else ref
    for spec in components or ():
        if getattr(spec, "key", None) == key:
            return spec
    return None


def _map_type_vocabularies() -> Tuple[FrozenSet[str], FrozenSet[str]]:
    """``(function-map types, direct-map types)`` — ASKED of the builders.

    Hand-listing these was wrong in BOTH directions: the list omitted
    ``map_function`` (a supported alias, so a valid declaration went silently
    inert and an impure family inside a retry region lost its replay-unsafe
    effect) and invented ``profile``, which no builder supports at all. Two
    spellings of one vocabulary is the same duplicate-authority defect as two
    spellings of one rule, and this was its fourth appearance in this slice —
    so the vocabulary is derived rather than corrected.

    ``MapScriptBuilder`` is DELIBERATELY absent. A script map's effect depends on
    what its embedded scripts do, and the only authority for that is the vetted
    registry; inspecting the map config would establish nothing. Script maps are
    therefore opaque by falling through, which is the correct answer rather than
    an omission.
    """
    from ..categories.components.builders.map_builder import (
        DirectMapBuilder,
        MapFunctionBuilder,
    )

    return (
        frozenset(MapFunctionBuilder.SUPPORTED_MAP_TYPES),
        frozenset(DirectMapBuilder.SUPPORTED_MAP_TYPES),
    )


def _effective_map_config(config: Mapping[str, Any], name: Optional[str]) -> dict:
    """The config the PLAN actually validates, not the one the caller typed.

    ``setdefault``, not a falsy check. `integration_builder` has nine
    `component_name` injection sites and EIGHT use `setdefault` — including the
    transform.map plan gate itself. The one falsy-form site is the apply-time
    literal-UUID drift re-validation, and copying that outlier made a
    present-but-falsy `component_name` derive a TRUSTED effect for a config the
    plan refuses. Measured: that change closed one misalignment and opened four.
    """
    effective = dict(config)
    if name:
        effective.setdefault("component_name", name)
    return effective


def _plan_would_build_this(
    config: Mapping[str, Any],
    *,
    name: Optional[str] = None,
    depends_on: Any = (),
    components_by_key: Optional[Mapping[str, Any]] = None,
) -> bool:
    """Whether the PLAN would build this map.

    ``validate_transform_map`` is the authority the plan itself calls, and
    ``validate_config`` is one of its ten ordered checks. Asking the builder
    directly meant asking a fragment: the plan supplies profile indexes that
    ``validate_config`` skips path-existence checks without, so
    ``MAP_FIELD_NOT_FOUND`` and ``MAP_PROFILE_REF_REQUIRED`` were invisible here.

    Asking the whole thing also DELETES machinery rather than adding it — the
    route dispatch and the reject-table re-check both live inside this function
    now. That is the direction that converges: each correction in this area has
    been smaller than the last, and this one removes the last hand-modelled
    fragment except the ``setdefault`` above.

    THE CEILING, stated rather than implied: this answers "the PLAN would build
    this config", not "this is what will execute". The two diverge in this
    subsystem — the raw-XML hatch is an apply-time bypass — so the raw-XML check
    stays separate above. A declaration can promise no more than the plan can
    establish, and the channel is documented to that bound.

    Conservative on error: an unanswerable question is a refusal.
    """
    from ..categories.components.builders.transform_map_validation import (
        validate_transform_map,
    )

    try:
        error = validate_transform_map(
            _effective_map_config(config, name),
            depends_on or [],
            dict(components_by_key or {}),
        )
    except Exception:  # noqa: BLE001 - an unanswerable question is a refusal
        return False
    if error is None:
        return True
    if getattr(error, "error_code", None) != "MAP_PROFILE_INDEX_UNAVAILABLE":
        return False
    # A profile-index refusal has THREE sources and only one is a question this
    # call cannot ask.
    #
    # The `$ref` branch reports the key it could not resolve, and that branch is
    # fully decidable from the `components_by_key` this call already supplies —
    # "the referenced component is missing, malformed, or not a profile" is an
    # answer, not an absence. Waving it through let a map naming a connection as
    # its profile derive a trusted, pure, replay-safe effect.
    #
    # The LITERAL existing-profile UUID branch reports no key, because there is
    # nothing here to resolve it against: the plan supplies caller-provided or
    # live-discovered indexes and this resolver has none. That one is deferred.
    #
    # Deferring rather than failing closed is deliberate, and it is not a
    # fail-open preference — it is accuracy. Inertness is NOT uniformly the safe
    # direction: for a claimed WRITE it establishes nothing and is conservative,
    # but a derived `replay_safe=False` raises a retry-safety ERROR where an
    # opaque map raises only a non-blocking warning, and a declared READ
    # disappears entirely from the strict classifiers, which iterate the trusted
    # set. Two of the three axes LOSE an error when a map goes inert.
    details = getattr(error, "details", None) or {}
    return "ref_key" not in details


def _join_cache_reads(config: Mapping[str, Any]):
    """The cache READS a map's ``document_cache_joins`` perform, or None if
    the join list cannot be modelled.

    Mirrors the repository's existing authority for this exact fact,
    ``cache_property_lineage._add_map_join_reads``, which records one cache read
    per join and marks a join carrying ``external_writer`` as externally
    satisfied. Deriving map effects without asking the joins made this module a
    SECOND, incomplete model of a fact that already had an owner — the defect
    class this issue exists to remove, reproduced inside the fix for it.

    Two joins are deliberately not recorded as reads, for the same reason a
    DEFAULTED property get records none: a contract read carries no
    "externally satisfied" flag, and lineage treats every contracted read as
    strict, so recording either would turn a valid flow into a false
    missing-writer error.

    * ``external_writer`` on the join — the cache is populated outside this
      process. The lineage downgrade reads the flag off the NODE, and a map
      semantic has no such attribute, so the warning branch is unreachable for a
      map and the read would always be fatal.
    * a LITERAL cache id — an existing account cache that nothing in this process
      writes, and no ``cache_ref`` in the IR spells it that way.
    """
    joins = config.get("document_cache_joins")
    if joins is None:
        return ()
    if not isinstance(joins, (list, tuple)):
        return None
    reads = []
    for join in joins:
        if not isinstance(join, Mapping):
            return None  # partial knowledge is never promoted to a complete effect
        cache_id = join.get("document_cache_id")
        if not isinstance(cache_id, str) or not cache_id.strip():
            return None
        cache_id = cache_id.strip()
        if join.get("external_writer") or not cache_id.startswith("$ref:"):
            continue
        reads.append(("cache", cache_id))
    return tuple(reads)


def derive_map_effect(
    config: Mapping[str, Any],
    *,
    substitutable: bool = False,
    name: Optional[str] = None,
    depends_on: Any = (),
    components_by_key: Optional[Mapping[str, Any]] = None,
) -> Optional[Tuple[tuple, tuple, bool]]:
    """``(reads, writes, replay_safe)`` for a map, or None when it is OPAQUE.

    Three independent ways to be opaque, and each closes a way of being wrong:

    * ``substitutable`` — the plan may bind an EXISTING live artifact instead of
      creating this one, so the config in hand is not the map that will execute.
      Its live content has not been inspected and is not version-bound, so
      nothing about it may be established.
    * an unrecognised or absent ``map_type`` — the config does not positively say
      what the map is.
    * any function whose effect is unannotated. Partial knowledge is never
      promoted to a complete effect: a map with one unknown function could read or
      write anything, and reporting the known half as the whole is worse than
      reporting nothing.
    """
    from ..categories.components.builders.map_function_registry import (
        get_function_family,
    )

    if substitutable or not isinstance(config, Mapping):
        return None
    # THE RAW-XML ESCAPE HATCH. `integration_builder` treats a config carrying
    # `xml` as "bypasses the structured builder entirely" and emits those bytes
    # verbatim, so `map_type` and `function_mappings` describe something that will
    # not run. The bytes themselves are not inspectable here, so opaque.
    if config.get("xml"):
        return None
    # Would the PLAN build this map? One question to the plan's own authority,
    # which subsumes the route dispatch, all four route-class tables, and the
    # nine other checks the plan runs.
    if not _plan_would_build_this(
        config, name=name, depends_on=depends_on, components_by_key=components_by_key
    ):
        return None
    function_types, direct_types = _map_type_vocabularies()
    map_type = config.get("map_type")
    # An authored value need not be hashable. `["direct"]` is a perfectly
    # possible thing for a caller to send, and a membership test on it raised
    # `TypeError: unhashable type` straight out of the tool with no machine code
    # at all. A non-string is simply not a recognised map type.
    if not isinstance(map_type, str):
        return None
    if map_type in direct_types:
        # A direct profile-to-profile map moves fields and touches no process
        # state — but ONLY if it really is one. `_DIRECT_ONLY_REJECT_KEYS` is the
        # builder's own statement of what a direct map may not carry, and a config
        # holding any of those keys is not a direct map the builder would accept.
        #
        # This branch previously answered before looking, which turned
        # `{map_type: "direct", function_mappings: [sequential_value]}` from a
        # refused declaration into an AGREED "pure and replay-safe" one — the
        # mirror of the defect that motivated deriving the vocabulary in the first
        # place, and worse in kind, because it agreed silently instead of warning.
        # So the reject keys are ASKED too, rather than assumed.
        # No reject-table re-check here: `_plan_would_build_this` already ran
        # every one of them through the plan's own authority. Re-asking would be
        # a second copy of a question already answered.
        #
        # A direct map may still carry document-cache joins, and those are READS.
        join_reads = _join_cache_reads(config)
        if join_reads is None:
            return None
        return (tuple(sorted(set(join_reads))), (), True)
    if map_type not in function_types:
        # Unrecognised, absent, or a form whose content this cannot establish
        # (a script map). Opaque.
        return None
    join_reads = _join_cache_reads(config)
    if join_reads is None:
        return None
    mappings = config.get("function_mappings")
    if mappings is None:
        return (tuple(sorted(set(join_reads))), (), True)
    if not isinstance(mappings, (list, tuple)):
        return None

    reads: List[Tuple[str, str]] = list(join_reads)
    writes: List[Tuple[str, str]] = []
    replay_safe = True
    for mapping in mappings:
        if not isinstance(mapping, Mapping):
            return None
        # THE BUILDER'S OWN LOOKUP, not a second copy of it. `get_function_family`
        # strips and lowercases; a raw dict lookup does neither, so a mapping the
        # builder happily emits (`" SEQUENTIAL_VALUE "`) would be unknown here.
        # The divergence fails closed — an unknown family makes the whole map
        # opaque — but two spellings of one lookup rule is the duplicate-authority
        # defect regardless, and the safe direction is not a reason to keep it.
        family = get_function_family(mapping.get("function_type"))
        if family is None or family.effect_kind is None:
            return None  # unknown or unannotated -> the whole map is opaque
        if family.effect_kind == "pure":
            continue
        if family.effect_kind == "impure_stateless":
            replay_safe = False
            continue
        parameters = mapping.get("parameters")
        if not isinstance(parameters, Mapping):
            return None
        # NOT `name` — that is this function's parameter, and shadowing it here
        # made the map's display name and a property name share one binding.
        target = parameters.get(family.effect_parameter)
        if not isinstance(target, str) or not target:
            return None
        if family.effect_kind == "property_get":
            # A DEFAULTED read cannot fail: the default establishes the value, so
            # the property needs no prior writer. A contract read carries no
            # has-default flag — `StateEffectV1.reads` is bare `(scope, name)` —
            # and lineage treats every contracted read as strict. Recording this
            # one would therefore turn a correct declaration into a false
            # PROPERTY_READ_BEFORE_WRITE on a flow that runs fine. Omitting it
            # establishes less, which is the safe direction.
            if parameters.get("default_value") is None:
                reads.append((family.effect_scope, target))
        elif family.effect_kind == "property_set":
            writes.append((family.effect_scope, target))
        else:  # pragma: no cover - closed vocabulary
            return None
    return (tuple(sorted(set(reads))), tuple(sorted(set(writes))), replay_safe)


#: Semantic kinds whose whole effect this derivation can account for.
#:
#: A CLOSED ALLOWLIST, not a denylist of opaque kinds. The denylist it replaces
#: named ``connector_call`` and never matched a ``source``/``target`` step,
#: whose semantic kind is ``connector`` — so the single most common thing a real
#: child contains was inspected and given an exact summary a caller could then
#: be TRUSTED against. An allowlist fails closed: a kind added to the compiler's
#: vocabulary later is inert here until it is deliberately classified.
#:
#: Membership is decided by ONE question, answered from the lineage authority:
#: does the walk account for this kind's STATE? ``map``, ``data_process`` and
#: ``process_call`` are the three kinds whose state effects are knowable only
#: through a typed contract — exactly the three `_opaque_reason` names — so only
#: they are excluded. Everything else is control flow, tracked state, a
#: terminal, or a document-shaping step.
#:
#: A connector is INSPECTABLE here even though it does external I/O, because
#: I/O is not state: `_reads_of`/`_writes_of` attribute no key to it, so it
#: cannot change either state answer. Excluding it instead — the first attempt
#: at this fix — was sound but very nearly vacuous: every legal root except a
#: control-only one contains a connector, so it made almost every real child
#: inert and cost the capability its reach. What a connector DOES decide is
#: replay safety, and that is where it is consulted.
#:
#: `test_the_inspectable_and_opaque_child_kinds_partition_the_vocabulary` pins
#: this bidirectionally against the compiler's own semantic-kind union, so a
#: nineteenth kind fails that test rather than silently landing on one side.
INSPECTABLE_CHILD_KINDS = frozenset({
    "branch",
    "connector",
    "connector_call",
    "cache_get",
    "cache_put",
    "cache_remove",
    "decision",
    "document_cache_retrieve",
    "exception",
    "flow_control",
    "message",
    "return_documents",
    "set_property",
    "stop",
    "try_catch",
})


def derive_subprocess_effect(child_ir: Any) -> Optional[Tuple[tuple, tuple, bool]]:
    """``(required_reads, must_writes, replay_safe)`` derived from a child's own IR.

    Every one of the three values is read off the compiler's OWN lineage walk
    (`walk_lineage`), which already models Branch leg ordering, the Decision
    meet and the catch fork. The scan this replaces re-derived path
    reachability with a regex on ``source_path`` that matched only root steps,
    and got two of the three answers wrong in the unsound direction:

    **``required_reads`` is a MAY set** — a key the child reads on ANY path
    before that path establishes it. The root-spine scan omitted a read inside
    a Branch, Decision or Try/Catch body entirely, so a child that genuinely
    depended on a caller-supplied key reported requiring nothing. A caller
    declaring the truthful read was rejected as a content mismatch, while a
    caller declaring nothing MATCHED and was trusted — the dependency vanished
    from the strict classifiers on both routes. Over-reporting a read that only
    one path takes is a demand the caller can always satisfy; under-reporting it
    is a build that passes and a runtime that does not.

    **``must_writes`` is a MUST set** — the meet over converging paths, so a
    write on one Decision arm only is correctly absent. This is the same
    guarantee the previous version aimed at, now obtained from the lattice that
    defines it rather than approximated by ignoring every non-root step.

    **``replay_safe`` is a MAY set too**, and for the same reason: a cache write
    or a persisted property ANYWHERE in the child — including inside a control
    body — makes re-running it observable. Spine-only was a false safety claim.

    **Opacity is INERT, not exact-empty.** A child containing anything outside
    `INSPECTABLE_CHILD_KINDS` has no derivable summary at all. Returning an
    empty-but-exact summary for such a child was an unsound ACCEPTANCE: it
    asserted "this child touches nothing" about a child whose contents were
    never inspected, and a caller declaration matching that fabrication was
    then trusted.
    """
    from ..compiler.process_ir.semantic_validation.context import (
        prepare_validation_context,
    )
    from ..compiler.process_ir.semantic_validation.lineage import walk_lineage
    from ..compiler.process_ir.contracts import SymbolTableV1

    try:
        prepared = prepare_validation_context(child_ir, SymbolTableV1(symbols=()))
    except Exception:  # pragma: no cover - an unlowerable child is simply opaque
        return None

    replay_safe = True
    for node in prepared.cfg.nodes:
        semantic = node.semantic
        kind = semantic.semantic_kind
        if kind not in INSPECTABLE_CHILD_KINDS:
            return None
        # A cache write is not replayable: re-running the child would write the
        # cache twice. A persisted process property survives the execution, so
        # replaying does not start from the same state. A connector does I/O
        # whose repetition is observable outside this process entirely. All
        # three are checked over EVERY node — a `cache_put` inside a Decision
        # arm still happens on the path that takes it.
        #
        # The connector rule is a deliberate UNDER-approximation: the
        # connector capability registry's `retry_safety` column is the real
        # authority and would clear a read-only operation, but resolving a
        # binding needs the symbol table a child summary does not carry.
        # Claiming replay-safety without that evidence is the unsound
        # direction; withholding it costs a missed opportunity.
        if kind in ("cache_put", "connector", "connector_call"):
            replay_safe = False
        elif kind == "set_property" and getattr(semantic, "persist", False):
            replay_safe = False

    walk = walk_lineage(prepared)
    return (walk.unestablished_reads, walk.established_at_exit, replay_safe)


# ---------------------------------------------------------------------------
# the boundary
# ---------------------------------------------------------------------------


def _as_internal_effect(reads, writes, replay_safe):
    from ..compiler.process_ir.semantic_validation.contracts import StateEffectV1

    return StateEffectV1(
        reads=tuple(tuple(pair) for pair in reads),
        writes=tuple(tuple(pair) for pair in writes),
        replay_safe=bool(replay_safe),
    )


def _declared(effect) -> Tuple[tuple, tuple, bool]:
    return (
        tuple(sorted((ref.scope, ref.name) for ref in effect.reads)),
        tuple(sorted((ref.scope, ref.name) for ref in effect.writes)),
        bool(effect.replay_safe),
    )


def resolve_process_ir_effect_declarations(
    process_roots: Sequence[Tuple[str, Any]],
    declarations: Any,
    symbols: Any,
    components: Sequence[Any] = (),
    child_roots: Optional[Mapping[str, Any]] = None,
    script_registry: Optional[Mapping] = None,
    conflict_policy: str = "reuse",
) -> EffectResolutionV1:
    """Verify identity, derive content server-side, and build per-root context.

    ``declarations`` may be ``None`` — the ordinary case — and then every root
    gets ``None`` for capabilities, which is byte-identical to the pre-#154 path.
    """
    from ..compiler.process_ir.semantic_validation.contracts import (
        ExternalWriterContractV1,
        MapEffectContractV1,
        ProcessIRValidationCapabilitiesV1,
        ScriptEffectContractV1,
        SubprocessSummaryV1,
    )
    from .vetted_scripts import lookup_vetted_script

    if declarations is None:
        return EffectResolutionV1({key: None for key, _ir in process_roots}, (), ())

    findings: List[EffectAuthorityFindingV1] = []
    inert: List[str] = []
    per_root: Dict[str, Any] = {}
    occurrences = {key: _occurrences(ir) for key, ir in process_roots}
    child_roots = dict(child_roots or {})

    # --- maps -------------------------------------------------------------
    map_rows: Dict[str, Any] = {}
    for index, item in enumerate(declarations.map_effects):
        pointer = "/effect_declarations/map_effects/{0}".format(index)
        bound = [key for key, found in occurrences.items() if item.map_ref in found["map_ref"]]
        if not bound:
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unbound"))
            continue
        symbol = _symbol(symbols, item.map_ref)
        if symbol is None or getattr(symbol, "component_type", None) != "transform.map":
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unresolved-or-wrong-type"))
            continue
        spec = _component(components, item.map_ref)
        derived = (
            derive_map_effect(
                getattr(spec, "config", None) or {},
                substitutable=_may_be_substituted(spec, conflict_policy),
                name=getattr(spec, "name", None),
                depends_on=getattr(spec, "depends_on", None) or [],
                components_by_key={
                    getattr(item, "key", None): item for item in (components or ())
                },
            )
            if spec
            else None
        )
        if derived is None:
            inert.append(pointer)
            continue
        if _declared(item.effect) != derived:
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "content-mismatch"))
            continue
        map_rows[item.map_ref] = (bound, MapEffectContractV1(
            map_ref=item.map_ref, effect=_as_internal_effect(*derived)
        ))

    # --- scripts ----------------------------------------------------------
    script_rows: List[Tuple[List[str], Any]] = []
    for index, item in enumerate(declarations.script_effects):
        pointer = "/effect_declarations/script_effects/{0}".format(index)
        matches = []
        for key, found in occurrences.items():
            for language, source in found["scripts"]:
                from .vetted_scripts import script_digest
                if language == item.language and "sha256:" + script_digest(source) == item.source_sha256:
                    matches.append((key, source))
        if not matches:
            # Either the script is not in this request at all, or the caller's
            # digest does not match the resolved source. Both are identity
            # failures and neither may be told apart in a value-free finding.
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unbound-or-digest-mismatch"))
            continue
        source = matches[0][1]
        contract = lookup_vetted_script(item.language, source, script_registry)
        if contract is None:
            inert.append(pointer)
            continue
        derived = (contract.reads, contract.writes, contract.replay_safe)
        if _declared(item.effect) != derived:
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "content-mismatch"))
            continue
        script_rows.append(([key for key, _ in matches], ScriptEffectContractV1(
            language=item.language,
            source_sha256=item.source_sha256[len("sha256:"):],
            effect=_as_internal_effect(*derived),
        )))

    # --- subprocesses -----------------------------------------------------
    subprocess_rows: List[Tuple[List[str], Any]] = []
    for index, item in enumerate(declarations.subprocess_effects):
        pointer = "/effect_declarations/subprocess_effects/{0}".format(index)
        bound = [key for key, found in occurrences.items() if item.process_ref in found["process_ref"]]
        if not bound:
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unbound"))
            continue
        symbol = _symbol(symbols, item.process_ref)
        if symbol is None or getattr(symbol, "component_type", None) != "process":
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unresolved-or-wrong-type"))
            continue
        child = child_roots.get(item.process_ref)
        if child is None:
            key = item.process_ref[len("$ref:"):] if item.process_ref.startswith("$ref:") else item.process_ref
            child = child_roots.get(key)
        derived = derive_subprocess_effect(child) if child is not None else None
        if derived is None:
            inert.append(pointer)  # reference-only child: nothing to inspect
            continue
        if _declared(item.effect) != derived:
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "content-mismatch"))
            continue
        subprocess_rows.append((bound, SubprocessSummaryV1(
            process_ref=item.process_ref, effect=_as_internal_effect(*derived)
        )))

    # --- external writers -------------------------------------------------
    writer_rows: List[Tuple[List[str], Any]] = []
    for index, item in enumerate(declarations.external_writers):
        pointer = "/effect_declarations/external_writers/{0}".format(index)
        # IDENTITY is "a cache_get in this root names this cache". The authored
        # `external_writer` flag governs the EFFECT — whether the missing-writer
        # error may downgrade — not whether the declaration is about a real thing.
        #
        # Gating identity on the flag rejected an unflagged-but-matching
        # declaration as unbound, which is a different answer from the one the
        # design gives: such a declaration is VALID and simply establishes
        # nothing. Turning "this proves nothing" into "your payload is invalid"
        # is the same overreach as trusting an unverified claim, pointed the
        # other way.
        bound = [
            key for key, found in occurrences.items()
            if item.cache_ref in found["cache_get_ref"]
        ]
        if not bound:
            # No cache_get in any root names this cache, so there is nothing for
            # the declaration to be about.
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unbound"))
            continue
        symbol = _symbol(symbols, item.cache_ref)
        if symbol is None or getattr(symbol, "component_type", None) != "documentcache":
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unresolved-or-wrong-type"))
            continue
        # Only the roots whose cache_get ALSO authors the flag receive the
        # contract: without the flag the declaration is inert there, and a
        # contract nothing can act on would only trigger the compiler's own
        # unbound-contract diagnostic.
        flagged = [
            key for key in bound
            if item.cache_ref in occurrences[key]["external_writer_ref"]
        ]
        if not flagged:
            inert.append(pointer)
            continue
        writer_rows.append((flagged, ExternalWriterContractV1(cache_ref=item.cache_ref)))

    if findings:
        # ALL-OR-NOTHING on error. A partially trusted context is a context whose
        # contents depend on which declaration happened to fail.
        return EffectResolutionV1({key: None for key, _ir in process_roots}, findings, inert)

    for key, _ir in process_roots:
        per_root[key] = ProcessIRValidationCapabilitiesV1(
            map_effects=tuple(
                row for bound, row in map_rows.values() if key in bound
            ),
            script_effects=tuple(row for bound, row in script_rows if key in bound),
            subprocess_summaries=tuple(row for bound, row in subprocess_rows if key in bound),
            external_writers=tuple(row for bound, row in writer_rows if key in bound),
        )
    return EffectResolutionV1(per_root, (), inert)


def effect_authority_rows() -> Tuple[Tuple[str, str], ...]:
    """The served statement of WHERE each declaration's content comes from.

    One row per declaration family, so a caller can read the trust boundary off
    the contract instead of inferring it from behaviour.
    """
    return (
        ("map_effects", "server-inspection:map-function-registry"),
        ("script_effects", "server-registry:vetted-scripts"),
        ("subprocess_effects", "server-inspection:child-process-ir"),
        ("external_writers", "caller-assertion:no-state-established"),
        ("omitted_or_inert", "none:every-strict-finding-stands"),
    )
