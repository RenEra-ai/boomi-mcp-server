"""Reference-phase collector: resolve component refs, accumulate findings (#143).

DARK in slice 2 — nothing calls this yet.

Scope boundary with #140
------------------------
A ``connector_call``'s ``operation_ref``/``connection_ref`` are NOT checked here.
They keep the specialized #140 codes
(``PROCESS_IR_REFERENCE_OPERATION_NOT_FOUND``,
``…CONNECTION_NOT_FOUND``, ``…CONNECTION_MISMATCH``), which say considerably
more than "a ref did not resolve" — they distinguish a missing operation from an
operation whose connection is unknown from a connection of the wrong connector
family. Re-reporting those conditions under the generic codes would be a
regression in diagnostic quality dressed up as unification, and the migration
matrix classifies that resolver `port-unchanged` for exactly this reason.

The two generic codes introduced here cover every OTHER component role: maps,
document caches, subprocess targets, and profiles.

Accumulate, don't fail fast
---------------------------
Each unresolved reference is recorded and the walk continues, so one bad ref does
not hide five others. What a failed reference DOES suppress is any later finding
that needed the missing fact — that is the dependency-scoped suppression rule,
and it is why this collector returns resolved facts alongside its findings
rather than just a list of problems.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, List, Optional, Tuple

from ....errors import (
    PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND,
    PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH,
)
from ..connector_resolution import (
    MAP_COMPONENT_TYPE,
    PROFILE_COMPONENT_TYPES,
)
from ..emitter_registry import (
    DP_PROFILE_COMPONENT_TYPE,
    SETPROP_PROFILE_TYPES,
)
from .contracts import ValidationDiagnosticV1
from .context import PreparedProcessValidationV1
from .findings import finding

_REFERENCE_PHASE = "reference"

CACHE_COMPONENT_TYPE = "documentcache"
PROCESS_COMPONENT_TYPE = "process"

#: The two profile containers declare their type in DIFFERENT vocabularies, and
#: both are imported from the emitter rather than re-stated here — the emitter
#: is the component that enforces them, so a local copy could only drift.
#:
#: * a Data Process split/combine step declares a bare KIND (``json``/``xml``)
#: * a Set-Properties profile source declares the FULL component type
#:   (``profile.json``/``profile.xml``/``profile.db``)
#:
#: Applying the Data Process map to both — which is how this first shipped —
#: leaves the Set-Properties half permanently inert, because ``profile.json``
#: is not a key of it, and makes ``profile.db`` unrepresentable.
def _declared_allowed(container: str, declared) -> FrozenSet[str]:
    """Component types satisfying one nested profile ref's own declaration.

    Fail-OPEN to the broad profile family when the declaration is absent or
    unrecognised: an unknown declaration is the emitter's to reject, and
    narrowing on a vocabulary this function does not understand would invent a
    reference error out of a value it cannot interpret.
    """
    raw = str(declared).strip().lower() if declared else ""
    if not raw:
        return PROFILE_COMPONENT_TYPES
    if container == "steps":
        mapped = DP_PROFILE_COMPONENT_TYPE.get(raw)
        return frozenset({mapped}) if mapped else PROFILE_COMPONENT_TYPES
    return frozenset({raw}) if raw in SETPROP_PROFILE_TYPES else PROFILE_COMPONENT_TYPES

#: Which component types satisfy each reference role. Closed sets, fail-closed:
#: an unlisted role is not validated here rather than being waved through under
#: a guessed type. ``profile_ref`` is the fallback for a source that declares no
#: kind; a step that DOES declare one is narrowed to it in ``_ref_roles``.
_ROLE_TYPES: Dict[str, FrozenSet[str]] = {
    "map_ref": frozenset({MAP_COMPONENT_TYPE}),
    "cache_ref": frozenset({CACHE_COMPONENT_TYPE}),
    "process_ref": frozenset({PROCESS_COMPONENT_TYPE}),
    "profile_ref": PROFILE_COMPONENT_TYPES,
}

#: Component-type CLASS reported as evidence. The concrete type is not evidence:
#: it is closed and safe, but the class is what a reader needs and keeping the
#: evidence vocabulary minimal keeps the redaction boundary small.
_TYPE_CLASS = {
    "map_ref": "map",
    "cache_ref": "cache",
    "process_ref": "process",
    "profile_ref": "profile",
}


class ResolvedReferenceFactsV1:
    """Refs that resolved cleanly, so later phases know what they may trust.

    Deliberately a plain object rather than a frozen model: it is built
    incrementally during one walk, never crosses a public boundary, and never
    reaches a report. ``unresolved`` is what drives dependency-scoped
    suppression downstream.
    """

    __slots__ = ("resolved", "unresolved")

    def __init__(self) -> None:
        self.resolved: Dict[str, str] = {}
        self.unresolved: set = set()

    def trusts(self, ref: Optional[str]) -> bool:
        return ref is not None and ref in self.resolved


def _ref_roles(semantic) -> Tuple[Tuple[str, str, str, FrozenSet[str]], ...]:
    """``(role, ref, path_suffix, allowed_component_types)`` for one CFG node.

    Connector operation/connection refs are deliberately excluded — see the
    module docstring.

    Profile refs are NESTED, not top-level: a ``data_process`` node carries one
    per split/combine step, and a ``set_property`` node carries one per profile
    source value. Walking only the top level would silently skip every profile
    reference in the payload, so both containers are descended explicitly.
    """
    pairs: List[Tuple[str, str, str, FrozenSet[str]]] = []
    for role in ("map_ref", "cache_ref", "process_ref"):
        value = getattr(semantic, role, None)
        if isinstance(value, str) and value:
            pairs.append((role, value, "", _ROLE_TYPES[role]))

    for container in ("steps", "source_values"):
        for index, item in enumerate(getattr(semantic, container, ()) or ()):
            nested = getattr(item, "profile_ref", None)
            if not isinstance(nested, str) or not nested:
                continue
            # The step declares its own profile type, so accept only the
            # matching component type. The broad profile set let a
            # json-declared split bind to profile.xml; the emitter rejects
            # that later, but as a compiler-defect code — for what is an
            # authored mis-binding. Resolved PER CONTAINER: the two surfaces
            # do not share a vocabulary.
            pairs.append(
                (
                    "profile_ref",
                    nested,
                    # each nested ref gets its OWN pointer, so two unresolved
                    # refs in one node are two findings, not one collapsed by
                    # dedup with neither step identified
                    "/{0}/{1}".format(container, index),
                    _declared_allowed(container, getattr(item, "profile_type", None)),
                )
            )

    return tuple(pairs)


def collect_reference_findings(
    prepared: PreparedProcessValidationV1,
) -> Tuple[Tuple[ValidationDiagnosticV1, ...], ResolvedReferenceFactsV1]:
    """Resolve every non-connector component reference in the prepared CFG."""
    findings: List[ValidationDiagnosticV1] = []
    facts = ResolvedReferenceFactsV1()

    # Node order, not dict order: the CFG's tuple order is the compiler's
    # deterministic order, so the walk itself is reproducible even before the
    # report is sorted.
    for node in prepared.cfg.nodes:
        for role, ref, suffix, allowed in _ref_roles(node.semantic):
            path = node.source_path + suffix
            symbol = prepared.symbol(ref)
            if symbol is None:
                facts.unresolved.add(ref)
                findings.append(
                    finding(
                        PROCESS_IR_REFERENCE_COMPONENT_NOT_FOUND,
                        "error",
                        _REFERENCE_PHASE,
                        path,
                        evidence=(("component_type_class", _TYPE_CLASS[role]),),
                        internal_node_id=node.node_id,
                    )
                )
                continue
            if symbol.component_type not in allowed:
                facts.unresolved.add(ref)
                findings.append(
                    finding(
                        PROCESS_IR_REFERENCE_COMPONENT_TYPE_MISMATCH,
                        "error",
                        _REFERENCE_PHASE,
                        path,
                        evidence=(("component_type_class", _TYPE_CLASS[role]),),
                        internal_node_id=node.node_id,
                    )
                )
                continue
            facts.resolved[ref] = symbol.component_type

    return tuple(findings), facts


__all__ = [
    "CACHE_COMPONENT_TYPE",
    "PROCESS_COMPONENT_TYPE",
    "ResolvedReferenceFactsV1",
    "collect_reference_findings",
]
