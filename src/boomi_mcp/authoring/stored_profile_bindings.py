"""The stored profile bindings of the maps and REST operations a request does NOT write (#159 S3).

A component apply reuses — bound by its declared id, by a name the component plan resolved, or
named only by reference — keeps the configuration the ACCOUNT stores; the request's own
configuration of it is discarded (``integration_builder.apply_writes_component_config``), so it
states no profile. Before this reader, such a map or operation therefore stated no profile at all,
and a flow whose map sits between two bound operations could not compile: the map's profile
continuity had nothing to compare (architect decision, #159 S3 bound-profile consultation).

This reads, at the id apply binds:

- a stored ``transform.map``'s ``Map/@fromProfile`` and ``Map/@toProfile``;
- a stored REST ``connector-action``'s ``requestProfile`` and ``responseProfile``, from the ONE
  ``GenericOperationConfig`` that settles its family and verb
  (``connector_resolution_snapshot.live_identity_from_component_xml``, the same reader and — through
  the phase's one ``BudgetedComponentReads``, which reads each id once — the same bytes);

after checking that the stored envelope names the bound id itself, and verifies each linked profile
from its own ComponentGet envelope: the envelope must state the
same component id and a real profile type. The compiler needs only that identity and type, so no
profile body is read. The result rides on the resolution snapshot
(``TrustedConnectorResolutionSnapshotV1.profile_bindings``) and is projected into the existing
symbol fields by ``recipes.materialization.build_symbol_table`` with the stored-profile identity
mechanism #186 introduced for document caches. Materialization reads nothing.

Never raises. A failed, unaffordable, unparseable or unjudged read, a malformed stored id, or an
envelope naming another id or type gives an UNKNOWN side, which states no profile, so the map
check refuses ``PROCESS_IR_SEMANTIC_PROFILE_MISMATCH`` at the consuming pointer before any write.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from .connector_resolution_snapshot import (
    StoredProfileBindingV1,
    StoredProfileSideV1,
    live_identity_from_component_xml,
)

_MAP_TYPE = "transform.map"
_OPERATION_TYPE = "connector-action"

_UNKNOWN = StoredProfileSideV1(state="unknown")
_ABSENT = StoredProfileSideV1(state="absent")


def _parse_strict(component_xml: Any, on_start: Callable[[list, str, dict], None]) -> bool:
    """Stream ``component_xml`` with expat, refusing any DOCTYPE or entity declaration.

    ``on_start(stack, local_name, attributes)`` sees every element with its ancestor stack
    (local names, the element itself last). False when the bytes are not a string, are empty,
    do not parse, or are not a ComponentGet ENVELOPE — the caller's answer is then UNKNOWN.

    The envelope is the platform's: its root is ``Component`` in the platform namespace
    (``arrange.model.BNS``; every capture this module reads, e.g.
    ``cap159-s3-bound-profiles/*.xml``). Any other root — ``<NotAComponent>`` carrying the
    expected ``componentId`` and ``type`` — answers for no component (#159 ARCH-r1 #3).
    """
    import xml.parsers.expat

    from ..arrange.model import BNS

    if not isinstance(component_xml, str) or not component_xml.strip():
        return False

    class _Refused(Exception):
        pass

    stack: list = []

    def _start(name, attributes):
        if not stack and name != f"{BNS}}}Component":
            raise _Refused
        stack.append(name.rsplit("}", 1)[-1].rsplit(":", 1)[-1])
        on_start(stack, stack[-1], attributes)

    def _end(_name):
        if stack:
            stack.pop()

    def _refuse(*_args, **_kwargs):
        raise _Refused

    parser = xml.parsers.expat.ParserCreate(namespace_separator="}")
    parser.EntityDeclHandler = _refuse
    parser.StartDoctypeDeclHandler = _refuse
    parser.StartElementHandler = _start
    parser.EndElementHandler = _end
    try:
        parser.Parse(component_xml, True)
    except (_Refused, xml.parsers.expat.ExpatError):
        return False
    return True


def stored_map_profile_links(component_xml: Any) -> Optional[Tuple[str, str]]:
    """``(fromProfile, toProfile)`` a stored map component declares, raw and stripped.

    ``""`` for a side the map names no profile on; ``None`` when the bytes are not ONE stored
    ``transform.map`` envelope holding exactly one ``Component/object/Map`` (the spelling the
    platform stores — capture ``cap184-fixtures/components/map.stored.xml``).
    """
    envelope_types = []
    maps = []

    def _on_start(stack, local, attributes):
        if len(stack) == 1:
            envelope_types.append(attributes.get("type"))
        if local == "Map" and len(stack) == 3 and stack[0] == "Component" and stack[1] == "object":
            maps.append((attributes.get("fromProfile"), attributes.get("toProfile")))

    if not _parse_strict(component_xml, _on_start):
        return None
    if envelope_types != [_MAP_TYPE] or len(maps) != 1:
        return None
    return tuple(value.strip() if isinstance(value, str) else "" for value in maps[0])


def _envelope_attributes(component_xml: Any) -> Optional[Dict[str, str]]:
    """The attributes of a stored component's envelope root, or None when unreadable."""
    roots = []

    def _on_start(stack, _local, attributes):
        if len(stack) == 1:
            roots.append(dict(attributes))

    if not _parse_strict(component_xml, _on_start) or len(roots) != 1:
        return None
    return roots[0]


def stored_profile_envelope(component_xml: Any) -> Optional[Tuple[Optional[str], Optional[str]]]:
    """``(componentId, type)`` of a stored component's own envelope, or None when unreadable.

    Read from the XML ROOT: the fetch dict's ``component_id`` falls back to the REQUESTED id when
    the envelope names none, so it cannot tell a profile from a component answering for another.
    """
    attributes = _envelope_attributes(component_xml)
    if attributes is None:
        return None
    return attributes.get("componentId"), attributes.get("type")


def _family_unsettled_in_scope(component, stored_xml) -> bool:
    """Whether a stored connector action whose bytes settle NO family is in this reader's scope.

    The platform states an operation's connector on its envelope's ``subType``; the one family
    reader (``connector_family_of``) maps it. An envelope stating no ``subType`` settles nothing
    — unlike a ``subType`` naming a family this repository does not model, which IS settled (as
    another family, out of scope, as before). Unsettled, the operation may be the REST operation
    the request declares, so it is in scope unless the request declares another family: recorded
    UNKNOWN, never dropped — a dropped reading leaves its input undeclared and unchecked
    (#159 ARCH-r1 #3).
    """
    from ..categories.components.builders.connector_builder import connector_family_of

    attributes = _envelope_attributes(stored_xml)
    if attributes is not None and str(attributes.get("subType") or "").strip():
        return False
    config = getattr(component, "config", None) or {}
    declared = config.get("connector_type") if isinstance(config, dict) else None
    return connector_family_of(declared) in (None, "rest")


def verified_profile_side(raw_profile_id: Optional[str], fetch_xml) -> StoredProfileSideV1:
    """One stored profile link, verified against the linked profile's own envelope.

    ``raw_profile_id`` is ``""`` for a side naming no profile (ABSENT) and ``None`` when the
    stored component settled nothing (UNKNOWN). ``fetch_xml(id) -> xml or None``.
    """
    from ..categories.integration_builder import canonical_component_id
    from ..compiler.process_ir.connector_resolution import PROFILE_COMPONENT_TYPES
    from ..connector_replay.ids import is_boomi_component_id

    if raw_profile_id is None:
        return _UNKNOWN
    if raw_profile_id == "":
        return _ABSENT
    wanted = canonical_component_id(raw_profile_id)
    # A stored id that is not a component id names no profile this request can match — and one
    # spelled like a plan placeholder would match a profile the request CREATES (ARCH-186-e1-03).
    if not wanted or not is_boomi_component_id(wanted):
        return _UNKNOWN
    envelope = stored_profile_envelope(fetch_xml(wanted))
    if envelope is None:
        return _UNKNOWN
    envelope_id, envelope_type = envelope
    if canonical_component_id(envelope_id) != wanted:
        return _UNKNOWN
    profile_type = envelope_type.strip().casefold() if isinstance(envelope_type, str) else ""
    if profile_type not in PROFILE_COMPONENT_TYPES:
        return _UNKNOWN
    return StoredProfileSideV1(
        state="known", profile_component_id=wanted, profile_component_type=profile_type
    )


def _declares_rest(component) -> bool:
    from ..categories.components.builders.connector_builder import connector_family_of

    config = getattr(component, "config", None) or {}
    return isinstance(config, dict) and connector_family_of(config.get("connector_type")) == "rest"


def _reads_as_rest(key, stored_xml) -> bool:
    """Whether stored bytes read as a REST operation by the one family reader, whatever their
    envelope id says."""
    if not isinstance(stored_xml, str) or not stored_xml:
        return False
    identity = live_identity_from_component_xml(key, stored_xml)
    return identity is not None and bool(identity.readable) and identity.family == "rest"


def stored_profile_bindings_for_unwritten_components(
    boomi_client,
    components,
    conflict_policy="reuse",
    existing_ids=None,
    reads=None,
) -> Optional[Tuple[StoredProfileBindingV1, ...]]:
    """The stored profile links of every map and REST operation apply does NOT write.

    Asked of apply's own predicates, as ``live_cache_profiles_for_unwritten_caches`` asks them:
    a component apply writes is described by the configuration apply submits and is skipped; any
    other is read at the component apply binds (``_bound_existing_id``). Every read goes through
    ``reads`` (the phase's one ``workflow.BudgetedComponentReads``), which reads each resolved id
    ONCE — so an operation's identity and its profile links come from the same bytes, and a
    profile two components link is read once. ``None`` with no client: nothing was read.

    A connector action is recorded only when it is a REST operation — proved by its stored bytes,
    or, when those could not be read or state no ``subType``, not declared as another family by the
    request (``_family_unsettled_in_scope``) — because only REST operations are scoped here; any
    other family keeps stating no profile, as before.

    Unlike the cache reader there is no ``binding_unjudged`` arm: a component apply writes is
    skipped even on a route whose name binding is unjudged (see the loop).
    """
    from ..categories.integration_builder import (
        _bound_existing_id,
        apply_writes_component_config,
        canonical_component_id,
    )

    if boomi_client is None:
        return None
    if reads is None:
        from .workflow import BudgetedComponentReads

        reads = BudgetedComponentReads(boomi_client)

    def _fetch_xml(component_id):
        fetched = reads.fetch(component_id)
        if not isinstance(fetched, dict):
            return None
        return fetched.get("xml")

    readings = []
    for component in components or ():
        key = getattr(component, "key", None)
        component_type = str(getattr(component, "type", "") or "").strip()
        if not isinstance(key, str) or not key or component_type not in (_MAP_TYPE, _OPERATION_TYPE):
            continue
        unknown_reading = StoredProfileBindingV1(
            component_key=key, component_type=component_type, input=_UNKNOWN, output=_UNKNOWN
        )
        if apply_writes_component_config(component, conflict_policy, existing_ids=existing_ids):
            # Described by the configuration apply submits — INCLUDING on a route whose name
            # binding is unjudged (the account held, the component plan not built). Unlike a
            # document cache (ARCH-186-e1-02), a created map or operation is NOT made unknown
            # there: measured, that refused the create control's compile that the degraded
            # plan/compile rule (owner decision 2026-09-14) keeps working, and typed apply
            # refuses an unjudged binding before any write. Left to the integrator (#159 S3b).
            continue
        component_id = canonical_component_id(_bound_existing_id(component, existing_ids))
        if component_id is None:
            if component_type == _MAP_TYPE or _declares_rest(component):
                readings.append(unknown_reading)
            continue
        unknown_reading = unknown_reading.model_copy(update={"component_id": component_id})
        stored_xml = _fetch_xml(component_id)
        # CDX-159-S3-r1-01: the stored bytes must be the BOUND component's own — apply references
        # ``component_id``, so links read from another component's envelope prove nothing. The
        # same envelope check ``verified_profile_side`` makes for every linked profile.
        envelope = stored_profile_envelope(stored_xml)
        if envelope is None or canonical_component_id(envelope[0]) != component_id:
            # Fail closed for every component this reader scopes: a map, or an operation REST by
            # its request OR by the answered bytes — the shared connector resolver may still read
            # them as REST, and recording nothing would leave its input undeclared, not unknown
            # (CDX-159-S3-r2-01). Since #159 ARCH-r1 #1 the one read FAILS an answer naming another
            # id, so no bytes arrive at all: bytes that are not the bound component's settle no
            # family, so an operation the request does not declare as another family stays in
            # scope (``_family_unsettled_in_scope`` with no bytes).
            if (
                component_type == _MAP_TYPE
                or _declares_rest(component)
                or _reads_as_rest(key, stored_xml)
                or _family_unsettled_in_scope(component, None)
            ):
                readings.append(unknown_reading)
            continue
        if component_type == _MAP_TYPE:
            links = stored_map_profile_links(stored_xml)
            if links is None:
                readings.append(unknown_reading)
                continue
            inbound, outbound = links
        else:
            identity = (
                live_identity_from_component_xml(key, stored_xml)
                if isinstance(stored_xml, str) and stored_xml
                else None
            )
            if identity is None or not identity.readable:
                if _declares_rest(component) or _family_unsettled_in_scope(component, stored_xml):
                    readings.append(unknown_reading)
                continue
            if identity.family != "rest":
                if _family_unsettled_in_scope(component, stored_xml):
                    readings.append(unknown_reading)
                # Another family is not scoped here; it states no profile, as before.
                continue
            if (identity.document_component_type or "").strip() != _OPERATION_TYPE:
                readings.append(unknown_reading)
                continue
            inbound, outbound = identity.rest_request_profile, identity.rest_response_profile
        readings.append(unknown_reading.model_copy(update={
            "input": verified_profile_side(inbound, _fetch_xml),
            "output": verified_profile_side(outbound, _fetch_xml),
        }))
    return tuple(sorted(readings, key=lambda item: item.component_key))


__all__ = [
    "stored_map_profile_links",
    "stored_profile_bindings_for_unwritten_components",
    "stored_profile_envelope",
    "verified_profile_side",
]
