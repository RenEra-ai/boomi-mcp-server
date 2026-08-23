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


def derive_map_effect(
    config: Mapping[str, Any],
    *,
    substitutable: bool = False,
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
        from ..categories.components.builders.map_builder import (
            _DIRECT_ONLY_REJECT_KEYS,
        )

        if any(key in config for key in _DIRECT_ONLY_REJECT_KEYS):
            return None
        return ((), (), True)
    if map_type not in function_types:
        # Unrecognised, absent, or a form whose content this cannot establish
        # (a script map). Opaque.
        return None
    mappings = config.get("function_mappings")
    if mappings is None:
        return ((), (), True)
    if not isinstance(mappings, (list, tuple)):
        return None

    reads: List[Tuple[str, str]] = []
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
        name = parameters.get(family.effect_parameter)
        if not isinstance(name, str) or not name:
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
                reads.append((family.effect_scope, name))
        elif family.effect_kind == "property_set":
            writes.append((family.effect_scope, name))
        else:  # pragma: no cover - closed vocabulary
            return None
    return (tuple(sorted(set(reads))), tuple(sorted(set(writes))), replay_safe)


def derive_subprocess_effect(child_ir: Any) -> Optional[Tuple[tuple, tuple, bool]]:
    """``(may_reads, must_writes, replay_safe)`` derived from a child's own IR.

    Reuses the lineage analysis's ``_reads_of``/``_writes_of`` against the child's
    lowered CFG, so this is not a second model of what counts as state.

    ``must_writes`` is deliberately a strict UNDER-approximation: only writes on
    the child's ROOT SPINE count. A write inside a Branch leg or a Decision arm
    happens on some paths and not others, and a summary that promised it would
    let a caller's later read be considered satisfied on a path where the write
    never ran. Root-spine steps precede any control node (a control terminalises
    its path), so they run on every normal exit.
    """
    from ..compiler.process_ir.semantic_validation import lineage as _lineage
    from ..compiler.process_ir.lowering import lower_process_ir_to_cfg

    try:
        cfg = lower_process_ir_to_cfg(child_ir)
    except Exception:  # pragma: no cover - an unlowerable child is simply opaque
        return None

    may_reads: List[Tuple[str, str]] = []
    must_writes: List[Tuple[str, str]] = []
    replay_safe = True
    for node in cfg.nodes:
        semantic = node.semantic
        for key, _has_default, _strict in _lineage._reads_of(semantic):
            may_reads.append(key)
        is_root_spine = _ROOT_STEP.fullmatch(node.source_path) is not None
        for key in _lineage._writes_of(semantic):
            if is_root_spine:
                must_writes.append(key)
        kind = semantic.semantic_kind
        if kind in ("connector_call", "process_call", "data_process", "map"):
            # A call, a nested call, an arbitrary script, or a map whose own
            # effect is not being derived here: none is provably replay safe.
            replay_safe = False
    return (
        tuple(sorted(set(may_reads))),
        tuple(sorted(set(must_writes))),
        replay_safe,
    )


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
        bound = [
            key for key, found in occurrences.items()
            if item.cache_ref in found["external_writer_ref"]
        ]
        if not bound:
            # Either no cache_get names this cache, or the one that does did not
            # author `external_writer`. The contract is meaningless without the
            # authored flag, so it does not bind.
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unbound"))
            continue
        symbol = _symbol(symbols, item.cache_ref)
        if symbol is None or getattr(symbol, "component_type", None) != "documentcache":
            findings.append(EffectAuthorityFindingV1(_INVALID, pointer, "unresolved-or-wrong-type"))
            continue
        writer_rows.append((bound, ExternalWriterContractV1(cache_ref=item.cache_ref)))

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
        ("omitted-or-inert", "none:every-strict-finding-stands"),
    )
